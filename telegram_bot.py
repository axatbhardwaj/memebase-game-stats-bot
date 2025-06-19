import os
import logging
import asyncio  # Added for running blocking function in thread
import io  # For in-memory file-like objects
import time  # Added for timestamped logging
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.constants import ParseMode  # Corrected import
from telegram.helpers import escape_markdown  # Import the escape_markdown helper
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
)
from dotenv import load_dotenv
from web3 import Web3  # For address validation

# Rich for text-based tables
from rich.console import Console as RichConsole
from rich.table import Table as RichTable
from rich.box import (
    ASCII_DOUBLE_HEAD as RICH_BOX_STYLE,
)  # New style with vertical lines

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
RPC_URLS_STR = os.getenv("RPC_URLS", os.getenv("RPC_URL"))
RPC_URLS = [url.strip() for url in RPC_URLS_STR.split(",")] if RPC_URLS_STR else []


# --- Import functions from investigation.py ---
from investigation import get_address_stats, EVENT_CONFIGS

# Conversation states
ASK_ADDRESS, ASK_EVENTS, ASK_DURATION = range(3)


def format_results_for_telegram(all_results, eth_rate, addresses, events, duration):
    """Formats the analysis results into a Markdown V2 string for Telegram."""

    # Event emojis for a bit of flair
    event_emojis = {
        "Hearted": "❤️",
        "Collected": "💰",
        "Summoned": "✨",
        "Unleashed": "🚀",
        "Purged": "🔥",
    }

    # Main title
    # Escape user-provided content once at the beginning
    addresses_str = ", ".join([f"`{escape_markdown(a, version=2)}`" for a in addresses])
    events_str = escape_markdown(events, version=2)
    duration_str = escape_markdown(str(duration), version=2)

    # Note on the ETH rate
    if eth_rate is not None:
        rate_str = f"*Current ETH to USD Rate:* `${escape_markdown(f'{eth_rate:,.2f}', version=2)}`"
    else:
        rate_str = "*ETH to USD Rate:* `Not Available`"

    # Header for the results block
    response_parts = [
        f"📊 *Memebase Stats*",
        f"\\- *Addresses:* {addresses_str}",
        f"\\- *Events:* {events_str}",
        f"\\- *Duration:* {duration_str} day\\(s\\)",
        rate_str,
        "\\- \\- \\- \\- \\- \\- \\- \\- \\- \\- \\- \\-",
    ]

    if not all_results:
        response_parts.append("\n*No data found for the selected criteria\\.*")
        return "\n".join(response_parts)

    for address, events_data in all_results.items():
        response_parts.append(f"\n*Address:* `{escape_markdown(address, version=2)}`")

        if not events_data:
            response_parts.append("\n_No activity found for this address\\._")
            continue

        for event_name, data in events_data.items():
            count = data.get("count", 0)
            emoji = event_emojis.get(event_name, "🔹")

            # Event Title with emoji and a newline for separation
            event_title = f"\n{emoji} *{escape_markdown(event_name, version=2)}*"
            response_parts.append(event_title)

            # Details are indented
            count_str = escape_markdown(str(count), version=2)
            response_parts.append(f"  `Count:` {count_str}")

            if count > 0:
                # Handle ETH value
                if data.get("total_amount_eth", 0) > 0:
                    eth_amount = data["total_amount_eth"] / 10**18
                    eth_str = f"{eth_amount:.6f}"
                    line = f"  `Total ETH:` `{escape_markdown(eth_str, version=2)}`"
                    if eth_rate is not None:
                        usd_value = eth_amount * eth_rate
                        # Using ≈ for approximate value
                        usd_str = f"\\(≈ ${escape_markdown(f'{usd_value:,.2f}', version=2)} USD\\)"
                        line += f" {usd_str}"
                    response_parts.append(line)

                # Handle token values
                if data.get("tokens"):
                    response_parts.append("  `Tokens:`")
                    for token_addr, amount in data["tokens"].items():
                        # Assuming 18 decimals for all tokens for simplicity
                        amount_normalized = amount / 10**18
                        amount_str = f"{amount_normalized:,.4f}"
                        token_addr_str = escape_markdown(token_addr, version=2)
                        line = f"    \\- `{token_addr_str}`: `{escape_markdown(amount_str, version=2)}`"
                        response_parts.append(line)

    full_response = "\n".join(response_parts)

    # Telegram has a message length limit of 4096 characters
    if len(full_response) > 4096:
        warning = "\n\n*Warning:* Output was truncated because it exceeded Telegram's message length limit\\."
        return full_response[: (4096 - len(warning))] + warning

    return full_response


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    welcome_message = (
        "Welcome to the Memebase Game Stats Bot!\n\n"
        "This bot fetches on-chain event statistics for the Memebase contract on the Base network. "
        "It analyzes events such as Hearted, Collected, Summoned, Unleashed, and Purged.\n\n"
        "The data presented can be filtered for the last 1 to 7 days of blockchain activity.\n\n"
        "To get started, use the /getstats command. You can provide a single Ethereum address or multiple addresses separated by commas."
    )
    await update.message.reply_text(welcome_message)


async def getstats_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the conversation to get stats for an address."""
    await update.message.reply_text(
        "Please enter the Ethereum address(es) you want to investigate (comma-separated):"
    )
    return ASK_ADDRESS


async def ask_address_received(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Stores the address and asks for event choices."""
    raw_addresses_input = update.message.text
    potential_addresses = [addr.strip() for addr in raw_addresses_input.split(",")]

    valid_addresses = []
    invalid_entries = []

    for addr_str in potential_addresses:
        if Web3.is_address(addr_str):
            valid_addresses.append(addr_str)
        elif (
            addr_str
        ):  # Not a valid address and not an empty string (e.g. from trailing comma)
            invalid_entries.append(addr_str)

    if not valid_addresses:
        await update.message.reply_text(
            "No valid Ethereum addresses provided. Please enter one or more valid addresses, separated by commas (e.g., 0x..., 0x...).\n"
            "You can use /cancel to stop."
        )
        return ASK_ADDRESS  # Stay in the same state

    context.user_data["addresses_to_investigate"] = valid_addresses
    logger.info(f"Valid addresses to investigate: {valid_addresses}")

    if invalid_entries:
        # Escaping each invalid entry individually before joining is safer
        escaped_invalid_entries = [
            escape_markdown(entry, version=2) for entry in invalid_entries
        ]
        invalid_str = f"`{escape_markdown(', '.join(invalid_entries), version=2)}`"  # The list itself as a code block
        # Use f-string carefully with already escaped parts, or build step-by-step
        warning_msg_part1 = escape_markdown(
            "The following entries were not valid addresses and will be ignored: ",
            version=2,
        )
        await update.message.reply_text(
            warning_msg_part1 + invalid_str, parse_mode=ParseMode.MARKDOWN_V2
        )

    event_options = ["All Events"]
    for key, config in EVENT_CONFIGS.items():
        event_options.append(f"{key}. {config['name']}")

    # Simple two-column layout for better readability if many events
    reply_keyboard = []
    for i in range(0, len(event_options), 2):
        reply_keyboard.append(event_options[i : i + 2])
    if len(event_options) % 2 == 1:  # Handle odd number of options
        pass  # Last item already added if odd

    await update.message.reply_text(
        "Which events do you want to analyze for the provided address(es)? You can select one from the list, or type 'All Events'.",
        reply_markup=ReplyKeyboardMarkup(
            reply_keyboard,
            one_time_keyboard=True,
            input_field_placeholder="Select event(s) or 'All Events'",
        ),
    )
    return ASK_EVENTS


async def ask_events_received(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Stores event choices and asks for duration."""
    user_id = update.effective_user.id if update.effective_user else "UnknownUser"
    current_time_start_func = time.time()
    logger.info(
        f"User {user_id}: Entered ask_events_received at {current_time_start_func:.4f}"
    )

    selected_event_choice_text = update.message.text
    addresses_to_investigate = context.user_data.get("addresses_to_investigate")

    if not addresses_to_investigate:
        logger.warning(
            f"User {user_id}: No addresses found in user_data in ask_events_received."
        )
        msg = escape_markdown(
            "Something went wrong, I don't have the address(es). Please start over with /getstats.",
            version=2,
        )
        await update.message.reply_text(
            msg, reply_markup=ReplyKeyboardRemove(), parse_mode=ParseMode.MARKDOWN_V2
        )
        return ConversationHandler.END

    logger.info(
        f"User {user_id}: Selected event choice for {addresses_to_investigate}: {selected_event_choice_text}"
    )

    selected_event_keys = []
    if selected_event_choice_text.lower() == "all events":
        selected_event_keys = list(EVENT_CONFIGS.keys())
    else:
        try:
            key_from_choice = selected_event_choice_text.split(".")[0].strip()
            if key_from_choice in EVENT_CONFIGS:
                selected_event_keys = [key_from_choice]
            else:
                msg = escape_markdown(
                    "Invalid event choice. Please try again or use /cancel.", version=2
                )
                await update.message.reply_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=ReplyKeyboardRemove(),
                )
                # Clean up partial data before ending
                if "addresses_to_investigate" in context.user_data:
                    del context.user_data["addresses_to_investigate"]
                return ConversationHandler.END
        except Exception:
            msg = escape_markdown(
                "Could not parse your event choice. Please use the format from the buttons or 'All Events'. Use /cancel to stop.",
                version=2,
            )
            await update.message.reply_text(
                msg,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=ReplyKeyboardRemove(),
            )
            if "addresses_to_investigate" in context.user_data:
                del context.user_data["addresses_to_investigate"]
            return ConversationHandler.END

    if not selected_event_keys:
        msg = escape_markdown(
            "No valid events selected. Please start over with /getstats.", version=2
        )
        await update.message.reply_text(
            msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=ReplyKeyboardRemove()
        )
        if "addresses_to_investigate" in context.user_data:
            del context.user_data["addresses_to_investigate"]
        return ConversationHandler.END

    context.user_data["selected_event_keys"] = selected_event_keys
    logger.info(f"User {user_id}: Stored selected_event_keys: {selected_event_keys}")

    duration_options = [f"{i} day{'s' if i > 1 else ''}" for i in range(1, 8)]
    reply_keyboard_duration = []
    # Simple two-column layout for duration options
    for i in range(0, len(duration_options), 2):
        reply_keyboard_duration.append(duration_options[i : i + 2])

    await update.message.reply_text(
        "For how many past days do you want to fetch stats? (1-7 days)",
        reply_markup=ReplyKeyboardMarkup(
            reply_keyboard_duration,
            one_time_keyboard=True,
            input_field_placeholder="Select duration (e.g., '3 days')",
        ),
    )
    return ASK_DURATION


async def ask_duration_received(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Processes duration choice and fetches stats."""
    user_id = update.effective_user.id if update.effective_user else "UnknownUser"
    current_time_start_func = time.time()
    logger.info(
        f"User {user_id}: Entered ask_duration_received at {current_time_start_func:.4f}"
    )

    selected_duration_text = update.message.text
    addresses_to_investigate = context.user_data.get("addresses_to_investigate")
    selected_event_keys = context.user_data.get("selected_event_keys")

    if not addresses_to_investigate or not selected_event_keys:
        missing_data_parts = []
        if not addresses_to_investigate:
            missing_data_parts.append("address(es)")
        if not selected_event_keys:
            missing_data_parts.append("selected events")

        logger.warning(
            f"User {user_id}: Missing data in ask_duration_received: {', '.join(missing_data_parts)}."
        )
        error_msg = escape_markdown(
            f"Something went wrong, I'm missing the {', '.join(missing_data_parts)}. Please start over with /getstats.",
            version=2,
        )
        await update.message.reply_text(
            error_msg,
            reply_markup=ReplyKeyboardRemove(),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        # Clean up all potentially stored data
        for key in ["addresses_to_investigate", "selected_event_keys", "duration_days"]:
            if key in context.user_data:
                del context.user_data[key]
        return ConversationHandler.END

    if not is_valid_duration(selected_duration_text):
        await update.message.reply_text(
            "Invalid duration. Please select one of the buttons (e.g., '3 days') or use /cancel.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_DURATION  # Ask again

    duration_days = is_valid_duration(selected_duration_text)
    context.user_data["duration_days"] = duration_days

    try:
        await update.message.reply_text(
            "Fetching your stats, this might take a moment...",
            reply_markup=ReplyKeyboardRemove(),
        )

        loop = asyncio.get_running_loop()
        start_time_stats = time.time()

        # Run the synchronous function in a separate thread
        all_analysis_results, eth_to_usd_rate, errors = await loop.run_in_executor(
            None,  # Use the default ThreadPoolExecutor
            get_address_stats,
            addresses_to_investigate,
            selected_event_keys,
            RPC_URLS,
            duration_days,
        )
        end_time_stats = time.time()
        logger.info(
            f"User {user_id}: get_address_stats call (awaited) completed at {end_time_stats:.4f}. Duration in thread (from this perspective): {end_time_stats - start_time_stats:.4f}s"
        )

        if errors:
            error_message = "Encountered some issues during the process:\n" + "\n".join(
                [f"- {escape_markdown(e, version=2)}" for e in errors]
            )
            await update.message.reply_text(
                error_message, parse_mode=ParseMode.MARKDOWN_V2
            )

        # Use the new formatting function
        selected_event_names = (
            "All Events"
            if len(selected_event_keys) == len(EVENT_CONFIGS)
            else ", ".join(
                [
                    EVENT_CONFIGS[key]["name"]
                    for key in selected_event_keys
                    if key in EVENT_CONFIGS
                ]
            )
        )

        response_message = format_results_for_telegram(
            all_analysis_results,
            eth_to_usd_rate,
            addresses_to_investigate,
            selected_event_names,
            duration_days,
        )

        await update.message.reply_text(
            response_message, parse_mode=ParseMode.MARKDOWN_V2
        )

    except Exception as e:
        logger.error(
            f"User {user_id}: Error during stats fetching or processing for addresses: {addresses_to_investigate}: {e}",
            exc_info=True,  # Log the full traceback
        )
        await update.message.reply_text(
            "An unexpected error occurred. Please try again or use /cancel."
        )

    finally:
        # Clean up user_data after the conversation ends
        if "addresses_to_investigate" in context.user_data:
            del context.user_data["addresses_to_investigate"]
        if "selected_event_keys" in context.user_data:
            del context.user_data["selected_event_keys"]
    return ConversationHandler.END


def is_valid_duration(text: str) -> int | None:
    try:
        duration_days = int(text.split()[0])
        if 1 <= duration_days <= 7:
            return duration_days
        else:
            return None
    except (ValueError, IndexError):
        return None


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    user = update.message.from_user
    user_id = user.id if user else "UnknownUser"
    logger.info(
        f"User {user_id} ({user.first_name if user else 'N/A'}) canceled the conversation."
    )
    # Clear all relevant user_data fields
    for key in ["addresses_to_investigate", "selected_event_keys", "duration_days"]:
        if key in context.user_data:
            del context.user_data[key]

    cancel_text = escape_markdown("Operation cancelled.", version=2)
    await update.message.reply_text(
        cancel_text,
        reply_markup=ReplyKeyboardRemove(),
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return ConversationHandler.END


def main() -> None:
    """Start the bot."""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is not set in the environment variables.")
        return
    if not RPC_URLS:
        logger.error(
            "RPC_URLS are not set in the environment variables. The bot may not function correctly."
        )

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("getstats", getstats_start)],
        states={
            ASK_ADDRESS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_address_received)
            ],
            ASK_EVENTS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_events_received)
            ],
            ASK_DURATION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_duration_received)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)

    logger.info("Bot starting...")
    application.run_polling()


if __name__ == "__main__":
    main()
