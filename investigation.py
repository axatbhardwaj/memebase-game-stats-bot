import web3

print(f"--- Loaded Web3.py version: {web3.__version__} ---")

import json
import os
from web3 import Web3
from dotenv import load_dotenv
import time
import math
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.table import Table
from rich.align import Align

load_dotenv()
start_time = time.time()
# --- Configuration (replace with your actual values) ---
RPC_URLS = [
    url.strip() for url in os.getenv("RPC_URLS", os.getenv("RPC_URL")).split(",")
]
CONTRACT_ADDRESS = "0x82A9c823332518c32a0c0eDC050Ef00934Cf04D4"
# ADDRESS_TO_INVESTIGATE = "0x39FCE6a33596b7319d7941F3F90d256574bcc954"
DEFAULT_MAX_RETRIES = 3  # Max retries for fetching a single chunk

# Map event names to their respective address and amount arguments in the ABI
EVENT_CONFIGS = {
    "1": {"name": "Hearted", "event_arg": "hearter", "amount_arg": "amount"},
    "2": {"name": "Collected", "event_arg": "hearter", "amount_arg": "allocation"},
    "3": {"name": "Summoned", "event_arg": "summoner", "amount_arg": "amount"},
    "4": {"name": "Unleashed", "event_arg": "unleasher", "amount_arg": "liquidity"},
    "5": {
        "name": "Purged",
        "event_arg": "memeToken",
        "amount_arg": "amount",
    },  # memeToken is the token address here
}

# --- Load ABI ---
from abi_memebase import memebase_abi

ABI = memebase_abi

# --- Connect to Ethereum Node ---
# Initialize w3 with the first RPC URL by default.
# The actual RPC URL used for fetching will be determined by check_rpc_urls and fetch_single_chunk
w3 = Web3(Web3.HTTPProvider(RPC_URLS[0] if RPC_URLS else "http://localhost:8545"))


def check_rpc_urls(rpc_urls_to_check):  # Renamed parameter to avoid conflict
    healthy_rpcs = []
    if not rpc_urls_to_check:
        print("No RPC URLs provided to check.")
        return []
    with ThreadPoolExecutor(max_workers=len(rpc_urls_to_check)) as executor:
        future_to_url = {
            executor.submit(Web3(Web3.HTTPProvider(url)).eth.get_block_number): url
            for url in rpc_urls_to_check
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
                healthy_rpcs.append(url)
                print(f"✅ RPC URL is healthy: {url}")
            except Exception as e:
                print(f"❌ RPC URL failed health check: {url} - Error: {e}")
    return healthy_rpcs


# Global contract instance, initialized after w3 is potentially updated by RPC check
# This might need to be initialized or re-initialized within get_address_stats if RPCs change
# For now, let's assume the first healthy RPC can be used for this initial contract object.
if RPC_URLS:
    first_healthy_rpc = check_rpc_urls(
        [RPC_URLS[0]]
    )  # Check only the first one for initial contract
    if first_healthy_rpc:
        w3 = Web3(Web3.HTTPProvider(first_healthy_rpc[0]))
        if not w3.is_connected():
            print("Failed to connect to Ethereum node. Please check your RPC_URL(s).")
            # exit() # Exit removed, function will return error or empty
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=ABI
        )
    else:
        print(
            "Initial RPC URL is not healthy. Contract object might not be functional."
        )
        contract = None  # Or handle error appropriately
else:
    print("No RPC URLs configured. Contract object might not be functional.")
    contract = None  # Or handle error appropriately


# Helper function to display progress - REMOVED as it's not suitable for library use.
# def display_progress(...):
#     ...


def fetch_single_chunk(
    ordered_rpcs_to_try: list[str],
    contract_address: str,
    abi: list,
    event_name: str,
    from_block: int,
    to_block: int,
    max_attempts_on_each_rpc: int = DEFAULT_MAX_RETRIES,
):
    for rpc_url in ordered_rpcs_to_try:
        attempts_on_this_rpc = 0
        while attempts_on_this_rpc < max_attempts_on_each_rpc:
            try:
                w3_instance = Web3(Web3.HTTPProvider(rpc_url))
                if not w3_instance.is_connected():
                    print(
                        f"\n🔌 Failed to connect to RPC: {rpc_url} when starting fetch for {event_name} (Blocks: {from_block}-{to_block}). Trying next RPC."
                    )
                    break  # Break from while loop to try next RPC in outer for-loop

                contract_instance = w3_instance.eth.contract(
                    address=Web3.to_checksum_address(contract_address), abi=abi
                )
                event_contract = getattr(contract_instance.events, event_name)()
                logs_chunk = event_contract.get_logs(
                    from_block=from_block, to_block=to_block
                )
                # print(f"\n✅ Successfully fetched {event_name} from {rpc_url} (Blocks: {from_block}-{to_block})") # Optional: for verbose success logging
                return logs_chunk  # Success
            except requests.exceptions.HTTPError as http_err:
                if http_err.response.status_code == 429:  # Too Many Requests
                    attempts_on_this_rpc += 1
                    print(
                        f"⚠️ Rate Limit (429) on {rpc_url} for {event_name} (Blocks: {from_block}-{to_block}). Retrying in 10s (Attempt {attempts_on_this_rpc}/{max_attempts_on_each_rpc} on this RPC)."
                    )
                    if attempts_on_this_rpc < max_attempts_on_each_rpc:
                        time.sleep(10)
                    else:
                        print(
                            f"Max retries for 429 reached on {rpc_url} for this chunk. Trying next RPC if available."
                        )
                        break  # Break from while loop to try next RPC
                else:
                    print(
                        f"\n❌ HTTP error {http_err.response.status_code} on {rpc_url} for {event_name} (Blocks: {from_block}-{to_block}): {http_err}. Trying next RPC."
                    )
                    break  # Break from while loop to try next RPC
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as conn_timeout_err:
                print(
                    f"\n❌ Connection/Timeout error on {rpc_url} for {event_name} (Blocks: {from_block}-{to_block}): {conn_timeout_err}. Trying next RPC."
                )
                break  # Break from while loop to try next RPC
            except Exception as e:
                print(
                    f"\n❌ Unexpected error on {rpc_url} for {event_name} (Blocks: {from_block}-{to_block}): {e}. Trying next RPC."
                )
                break  # Break from while loop to try next RPC
        # If the while loop for this RPC completed (either max 429 retries or other error),
        # the outer for-loop will try the next rpc_url if available.

    print(
        f"\n❌ Exhausted all RPCs for chunk {from_block}-{to_block} ({event_name}). Last attempt was on {ordered_rpcs_to_try[-1] if ordered_rpcs_to_try else 'N/A'}."
    )
    return []  # Return empty list if all RPCs failed for this chunk


# Helper function to fetch logs in chunks
def fetch_event_logs_in_chunks(
    contract_obj,  # Changed from contract to contract_obj to avoid conflict
    event_name,
    start_block,
    end_block,
    max_range_per_request,
    rpc_urls_to_use,  # Renamed from rpc_urls
):
    """
    Fetches logs for a specific event from a contract over a large block range
    by breaking it into smaller chunks and fetching them in parallel.
    """
    all_logs = []
    # total_chunks calculation removed as display_progress is removed
    # start_overall_time = time.time() # This timing is internal, not for display_progress
    completed_chunks = 0

    if not rpc_urls_to_use:
        print(f"No RPC URLs available to fetch logs for {event_name}.")
        return []
    if not contract_obj:
        print(f"Contract object not initialized. Cannot fetch logs for {event_name}.")
        return []

    num_rpcs = len(rpc_urls_to_use)
    total_blocks = end_block - start_block + 1
    # blocks_per_rpc = math.ceil(total_blocks / num_rpcs) # This was for segmenting block ranges by RPC, which is less critical now
    # as each task gets a rotated list of all RPCs.
    # However, it can still be used for initial distribution if desired,
    # but the ThreadPoolExecutor will manage concurrency.
    # For simplicity, we can create tasks for each max_range_per_request chunk directly.

    tasks = []
    # Task arguments: (ordered_rpcs_for_chunk, contract_address, abi, event_name, from_block, to_block, max_attempts_on_each_rpc)

    # Iterate directly over the total block range, creating chunks of max_range_per_request
    for current_from_block in range(start_block, end_block + 1, max_range_per_request):
        current_to_block = min(
            current_from_block + max_range_per_request - 1, end_block
        )

        if (
            not rpc_urls_to_use
        ):  # Should not happen if check is done before calling this func
            print(
                f"Error: No RPC URLs available for task generation for {event_name}. Skipping chunk {current_from_block}-{current_to_block}"
            )
            continue

        # Rotate the list of RPCs for the current chunk to vary the starting RPC
        current_task_idx = len(tasks)
        num_available_rpcs = len(rpc_urls_to_use)

        start_index = current_task_idx % num_available_rpcs
        ordered_rpcs_for_chunk = (
            rpc_urls_to_use[start_index:] + rpc_urls_to_use[:start_index]
        )

        tasks.append(
            (
                ordered_rpcs_for_chunk,
                contract_obj.address,
                ABI,
                event_name,
                current_from_block,
                current_to_block,
                DEFAULT_MAX_RETRIES,  # This is max_attempts_on_each_rpc
            )
        )

    if not tasks:
        print(
            f"No tasks generated for fetching {event_name} logs. Block range might be too small or invalid."
        )
        return []

    with ThreadPoolExecutor(max_workers=len(rpc_urls_to_use)) as executor:
        future_to_chunk = {
            executor.submit(fetch_single_chunk, *task): task for task in tasks
        }

        for future in as_completed(future_to_chunk):
            chunk_range_task = future_to_chunk[future]
            try:
                logs_chunk = future.result()
                if (
                    logs_chunk
                ):  # Ensure logs_chunk is not None or empty before extending
                    all_logs.extend(logs_chunk)
            except Exception as exc:
                print(
                    f"Chunk {chunk_range_task[4]}-{chunk_range_task[5]} for {event_name} generated an exception: {exc}"
                )
            # Removed display_progress call
            # finally:
            #     completed_chunks += 1
            #     display_progress(
            #         completed_chunks,
            #         len(tasks),
            #         start_overall_time, # This would need to be passed or managed differently
            #         event_name,
            #         start_block, # These were overall range, not chunk specific
            #         end_block,
            #     )

    # sys.stdout.write("\\n") # Removed, no progress bar to clear
    # sys.stdout.flush() # Removed
    return all_logs


def analyze_event_logs(logs, address_to_find, event_arg, amount_arg):
    """
    Counts how many times a specific address has 'hearted' and the total amount.
    """
    count = 0
    total_amount = 0
    # Ensure the address to find is checksummed for accurate comparison
    checksum_address_to_find = Web3.to_checksum_address(address_to_find)
    for log in logs:
        if getattr(log.args, event_arg) == checksum_address_to_find:
            count += 1
            total_amount += getattr(log.args, amount_arg)
    return {"count": count, "total_amount": total_amount}


def fetch_eth_to_usd_rate():
    primary_url = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/eth.json"
    fallback_url = "https://latest.currency-api.pages.dev/v1/currencies/eth.json"

    try:
        response = requests.get(primary_url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        return data["eth"]["usd"]
    except requests.exceptions.RequestException as e:
        print(f"Primary API failed: {e}. Trying fallback URL...")
        try:
            response = requests.get(fallback_url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()
            return data["eth"]["usd"]
        except requests.exceptions.RequestException as e:
            print(f"Fallback API also failed: {e}. Cannot fetch ETH to USD rate.")
            return None


# Define the total range and chunk size based on the problem statement
TOTAL_BLOCKS_TO_FETCH = 432000  # on base blockchain 1 day = 432000 blocks
MAX_BLOCK_RANGE_PER_REQUEST = 500  # Alchemy's 500 block limit


def get_address_stats(
    addresses_to_investigate_list, selected_event_keys_list, custom_rpc_urls=None
):
    """
    Fetches and analyzes event logs for specified addresses and events.

    Args:
        addresses_to_investigate_list (list): List of Ethereum addresses (strings).
        selected_event_keys_list (list): List of event keys (e.g., "1", "2") from EVENT_CONFIGS.
        custom_rpc_urls (list, optional): List of RPC URLs to use. Defaults to global RPC_URLS.

    Returns:
        tuple: (all_analysis_results, eth_to_usd_rate, errors)
               all_analysis_results (dict): Nested dictionary with stats per address per event.
               eth_to_usd_rate (float/None): ETH to USD conversion rate.
               errors (list): List of error messages encountered.
    """
    global RPC_URLS, w3, contract  # Allow modification of global w3 and contract if custom_rpc_urls are better

    errors = []
    all_analysis_results = {}

    current_rpc_urls = custom_rpc_urls if custom_rpc_urls else RPC_URLS

    print("\n--- Checking RPC URL Health ---")
    healthy_rpc_urls = check_rpc_urls(current_rpc_urls)
    if not healthy_rpc_urls:
        errors.append("No healthy RPC URLs available. Cannot proceed.")
        print("No healthy RPC URLs available. Exiting analysis function.")
        return {}, None, errors

    # Update global w3 and contract if healthy_rpc_urls are different or provide a better primary
    if healthy_rpc_urls[0] != (
        w3.provider.endpoint_uri if w3 and w3.provider else None
    ):
        print(f"Updating w3 instance to use: {healthy_rpc_urls[0]}")
        w3 = Web3(Web3.HTTPProvider(healthy_rpc_urls[0]))
        if not w3.is_connected():
            error_msg = (
                "Failed to connect to Ethereum node with the selected healthy RPC."
            )
            errors.append(error_msg)
            print(error_msg)
            return {}, None, errors
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=ABI
        )
        print("Contract object re-initialized with new RPC.")
    elif not contract:  # If contract was not initialized initially
        if w3 and w3.is_connected():
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=ABI
            )
            print("Contract object initialized.")
        else:
            error_msg = "w3 is not connected, cannot initialize contract."
            errors.append(error_msg)
            print(error_msg)
            return {}, None, errors

    eth_to_usd_rate = fetch_eth_to_usd_rate()
    if eth_to_usd_rate is None:
        errors.append("Failed to fetch ETH to USD rate. USD values will be N/A.")

    # Determine the overall block range for investigation
    try:
        current_block_number = w3.eth.block_number
        # end_block_overall = current_block_number # Use actual current block for most up-to-date
        end_block_overall = (
            31589310  # Using fixed for consistency during dev, change as needed
        )
        start_block_overall = max(0, end_block_overall - TOTAL_BLOCKS_TO_FETCH)
        print(
            f"Log investigation range: block {start_block_overall} to {end_block_overall} (total {end_block_overall - start_block_overall + 1} blocks)"
        )
    except Exception as e:
        error_msg = (
            f"Failed to get current block number: {e}. Cannot determine block range."
        )
        errors.append(error_msg)
        print(error_msg)
        return {}, eth_to_usd_rate, errors

    if not contract:  # Double check contract after RPC and w3 setup
        error_msg = "Contract object is not initialized. Cannot fetch logs."
        errors.append(error_msg)
        print(error_msg)
        return {}, eth_to_usd_rate, errors

    for event_key in selected_event_keys_list:
        if event_key in EVENT_CONFIGS:
            event_info = EVENT_CONFIGS[event_key]
            event_name = event_info["name"]
            event_arg = event_info["event_arg"]
            amount_arg = event_info["amount_arg"]

            print(f"--- Fetching {event_name} Logs ---")
            # Pass the globally managed (and potentially updated) contract object
            logs = fetch_event_logs_in_chunks(
                contract,  # Use the module-level contract object
                event_name,
                start_block_overall,
                end_block_overall,
                MAX_BLOCK_RANGE_PER_REQUEST,
                healthy_rpc_urls,  # Use the confirmed healthy RPCs
            )

            if not logs:
                print(
                    f"No logs found for {event_name}. It's possible there were no such events in the range, or an error occurred."
                )
                # We can add an error to the list if logs is None, indicating a fetch problem
                # If logs is an empty list, it means successful fetch but no events.

            for address in addresses_to_investigate_list:
                # Ensure address entry exists in results
                if address not in all_analysis_results:
                    all_analysis_results[address] = {}

                analysis_results = analyze_event_logs(
                    logs, address, event_arg, amount_arg
                )

                eth_amount = (
                    analysis_results["total_amount"] / 10**18
                )  # Assuming 18 decimals
                usd_value = None
                if eth_to_usd_rate is not None:
                    usd_value = eth_amount * eth_to_usd_rate

                all_analysis_results[address][event_name] = {
                    "count": analysis_results["count"],
                    "total_amount_eth": eth_amount,
                    "total_amount_usd": usd_value,
                }
        else:
            warn_msg = f"Warning: Invalid event selection: {event_key}. Skipping."
            print(warn_msg)
            errors.append(warn_msg)

    return all_analysis_results, eth_to_usd_rate, errors


# --- Original main execution block (commented out or removed) ---
# current_block_number = w3.eth.block_number
# # end_block_overall = current_block_number
# end_block_overall = 31589310
# start_block_overall = max(
#     0, end_block_overall - TOTAL_BLOCKS_TO_FETCH
# )  # Ensure block number doesn't go below 0
#
# print(
#     f"Starting log investigation from block {start_block_overall} to {end_block_overall} (total {end_block_overall - start_block_overall + 1} blocks)"
# )
#
# # Get user input for events to analyze
# print(
#     "\nSelect events to analyze (comma-separated numbers, or type '*' for all events):"
# )
# for key, config in EVENT_CONFIGS.items():
#     print(f"{key}. {config['name']}")
#
# event_choices_input = input("Enter your choices: ")
#
# if event_choices_input.lower() == "*":
#     selected_event_keys = list(EVENT_CONFIGS.keys())
# else:
#     selected_event_keys = [key.strip() for key in event_choices_input.split(",")]
#
# # Get user input for addresses to investigate
# addresses_input = input("\nEnter addresses to investigate (comma-separated): ")
# ADDRESSES_TO_INVESTIGATE = [addr.strip() for addr in addresses_input.split(",")]
#
# # --- Health Check for RPCs ---
# print("\n--- Checking RPC URL Health ---")
# RPC_URLS = check_rpc_urls(RPC_URLS)
# if not RPC_URLS:
#     print("No healthy RPC URLs available. Exiting.")
#     exit()
#
# # --- Analysis ---
# eth_to_usd_rate = fetch_eth_to_usd_rate()
#
# all_analysis_results = {}
#
# console = Console()
#
# for event_key in selected_event_keys:
#     if event_key in EVENT_CONFIGS:
#         event_info = EVENT_CONFIGS[event_key]
#         event_name = event_info["name"]
#         event_arg = event_info["event_arg"]
#         amount_arg = event_info["amount_arg"]
#
#         print(f"\n--- Fetching {event_name} Logs ---")
#         logs = fetch_event_logs_in_chunks(
#             contract,
#             event_name,
#             start_block_overall,
#             end_block_overall,
#             MAX_BLOCK_RANGE_PER_REQUEST,
#             RPC_URLS,
#         )
#
#         for address in ADDRESSES_TO_INVESTIGATE:
#             if address not in all_analysis_results:
#                 all_analysis_results[address] = {}
#
#             analysis_results = analyze_event_logs(logs, address, event_arg, amount_arg)
#
#             eth_amount = analysis_results["total_amount"] / 10**18
#             usd_value = None
#             if eth_to_usd_rate is not None:
#                 usd_value = eth_amount * eth_to_usd_rate
#
#             all_analysis_results[address][event_name] = {
#                 "count": analysis_results["count"],
#                 "total_amount_eth": eth_amount,
#                 "total_amount_usd": usd_value,
#             }
#     else:
#         print(f"Warning: Invalid event selection: {event_key}. Skipping.")
#
# end_time = time.time()
# print(f"Time taken: {end_time - start_time:.2f} seconds") # start_time was global
#
# console.print("\n--- Analysis Results ---")
#
# for address, events_data in all_analysis_results.items():
#     table = Table(
#         title=f"Results for Address: {address}",
#         show_lines=True,
#         title_style="bold magenta",
#     )
#
#     table.add_column("Event Name", style="cyan", no_wrap=False)
#     table.add_column("Count", style="magenta", justify="center")
#     table.add_column("Total Amount ETH", style="green", justify="right")
#     table.add_column("Total Amount USD", style="yellow", justify="right")
#
#     for event_name, data in events_data.items():
#         eth_str = f'{data["total_amount_eth"]:.6f}'
#         usd_str = (
#             f'{data["total_amount_usd"]:.2f}'
#             if data["total_amount_usd"] is not None
#             else "N/A"
#         )
#         table.add_row(event_name, str(data["count"]), eth_str, usd_str)
#     console.print(Align.center(table))

# Example of how to call the new function (for testing, can be removed)
# if __name__ == "__main__":
#     print("Testing get_address_stats...")
#     test_addresses = ["0x39FCE6a33596b7319d7941F3F90d256574bcc954"] # Replace with a test address
#     test_event_keys = ["1", "3"] # Test with Hearted and Summoned
#
#     # Ensure RPC_URLS is loaded from .env or set directly for testing
#     if not RPC_URLS or not RPC_URLS[0]:
#         print("RPC_URLS are not configured. Please set them in .env or directly.")
#     else:
#         results, rate, errors_list = get_address_stats(test_addresses, test_event_keys)
#
#         if errors_list:
#             print("\n--- Errors Encountered ---")
#             for err in errors_list:
#                 print(err)
#
#         print("\n--- Test Results ---")
#         if results:
#             for addr, data in results.items():
#                 print(f"Address: {addr}")
#                 for event_n, event_d in data.items():
#                     print(f"  Event: {event_n}")
#                     print(f"    Count: {event_d['count']}")
#                     print(f"    ETH: {event_d['total_amount_eth']:.6f}")
#                     print(f"    USD: {event_d['total_amount_usd']:.2f}" if event_d['total_amount_usd'] is not None else "    USD: N/A")
#         else:
#             print("No results returned.")
#
#         if rate is not None:
#             print(f"\nETH to USD Rate: {rate}")
#         else:
#             print("\nETH to USD Rate: Not Available")
