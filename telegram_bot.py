import os
import logging
import asyncio  # Added for running blocking function in thread
import io  # For in-memory file-like objects
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
ASK_ADDRESS, ASK_EVENTS = range(2)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    welcome_message = (
        "Welcome to the Memebase Game Stats Bot!\n\n"
        "This bot fetches on-chain event statistics for the Memebase contract on the Base network. "
        "It analyzes events such as Hearted, Collected, Summoned, Unleashed, and Purged.\n\n"
        "📊 The data presented is for approximately the last 7 days of blockchain activity.\n\n"
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
    """Processes event choices and fetches stats."""
    selected_event_choice_text = update.message.text
    addresses_to_investigate = context.user_data.get("addresses_to_investigate")

    if not addresses_to_investigate:
        msg = escape_markdown(
            "Something went wrong, I don't have the address(es). Please start over with /getstats.",
            version=2,
        )
        await update.message.reply_text(
            msg, reply_markup=ReplyKeyboardRemove(), parse_mode=ParseMode.MARKDOWN_V2
        )
        return ConversationHandler.END

    logger.info(
        f"Selected event choice for {addresses_to_investigate}: {selected_event_choice_text}"
    )

    escaped_choice_for_fetching_msg = escape_markdown(
        selected_event_choice_text, version=2
    )
    num_addresses = len(addresses_to_investigate)
    address_plural = "address" if num_addresses == 1 else "addresses"

    fetching_text_intro = escape_markdown(
        f"Fetching stats for {num_addresses} {address_plural} concerning ", version=2
    )
    fetching_text_choice = f"`'{escaped_choice_for_fetching_msg}'`"
    fetching_text_suffix = escape_markdown(
        "... Please wait, this may take a moment.", version=2
    )
    fetching_text = fetching_text_intro + fetching_text_choice + fetching_text_suffix
    await update.message.reply_text(
        fetching_text,
        reply_markup=ReplyKeyboardRemove(),
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    selected_event_keys = []
    if selected_event_choice_text.lower() == "all events":
        selected_event_keys = list(EVENT_CONFIGS.keys())
    else:
        # Try to parse the choice like "1. Hearted"
        try:
            key_from_choice = selected_event_choice_text.split(".")[0].strip()
            if key_from_choice in EVENT_CONFIGS:
                selected_event_keys = [key_from_choice]
            else:
                msg = escape_markdown(
                    "Invalid event choice. Please try again or use /cancel.", version=2
                )
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
                if "addresses_to_investigate" in context.user_data:
                    del context.user_data["addresses_to_investigate"]
                return ConversationHandler.END
        except Exception:
            msg = escape_markdown(
                "Could not parse your event choice. Please use the format from the buttons or 'All Events'. Use /cancel to stop.",
                version=2,
            )
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
            if "addresses_to_investigate" in context.user_data:
                del context.user_data["addresses_to_investigate"]
            return ConversationHandler.END

    if not selected_event_keys:
        msg = escape_markdown(
            "No valid events selected. Please start over with /getstats.", version=2
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)
        if "addresses_to_investigate" in context.user_data:
            del context.user_data["addresses_to_investigate"]
        return ConversationHandler.END

    try:
        # Run the blocking function in a separate thread
        all_analysis_results, eth_to_usd_rate, errors = await asyncio.to_thread(
            get_address_stats,
            addresses_to_investigate,
            selected_event_keys,
            custom_rpc_urls=RPC_URLS,
        )

        response_message_parts = []

        if eth_to_usd_rate is not None:
            rate_str = escape_markdown(f"{eth_to_usd_rate:.2f}", version=2)
            response_message_parts.append(f"_Current ETH to USD Rate: ${rate_str}_\n\n")
        else:
            response_message_parts.append(
                escape_markdown("_ETH to USD rate not available._", version=2) + "\n\n"
            )

        for address_item in addresses_to_investigate:
            escaped_current_address = escape_markdown(address_item, version=2)
            response_message_parts.append(
                f"*Analysis for Address:* `{escaped_current_address}`\n"
            )

            if (
                address_item in all_analysis_results
                and all_analysis_results[address_item]
            ):
                address_data = all_analysis_results[address_item]

                # Use Rich to generate a plain text table with a box style that includes vertical lines
                rich_text_table = RichTable(
                    box=RICH_BOX_STYLE,
                    show_header=True,
                    show_lines=True,
                    padding=(0, 1),
                    title_style="",
                    header_style="",
                )
                rich_text_table.add_column(
                    "Event", justify="left", min_width=12, overflow="fold"
                )
                rich_text_table.add_column("Count", justify="right")
                rich_text_table.add_column("Total ETH", justify="right")
                rich_text_table.add_column("Total USD", justify="right")

                for event_name_from_results, data in address_data.items():
                    eth_str = f"{data['total_amount_eth']:.6f}"
                    usd_str = (
                        f"${data['total_amount_usd']:.2f}"
                        if data["total_amount_usd"] is not None
                        else "N/A"
                    )
                    rich_text_table.add_row(
                        str(event_name_from_results),
                        str(data["count"]),
                        eth_str,
                        usd_str,
                    )

                # Capture plain text output of the rich table
                plain_table_io = io.StringIO()
                # Corrected Console instantiation for plain text:
                # No force_plain, ensure color_system is None (or not set, defaults to auto-detection which for a StringIO is usually no color)
                # Set record=False explicitly as we are not exporting SVG/HTML.
                console = RichConsole(
                    file=plain_table_io, record=False, color_system=None, width=80
                )  # Adjust width if needed
                console.print(rich_text_table)
                plain_table_str = plain_table_io.getvalue()
                plain_table_io.close()

                safe_plain_table_str = plain_table_str.replace("```", "``·")

                response_message_parts.append(
                    "```\n" + safe_plain_table_str.strip() + "\n```\n\n"
                )
            else:
                no_data_msg = escape_markdown(
                    "  No specific event data found for this address with the selected criteria.",
                    version=2,
                )
                response_message_parts.append(no_data_msg + "\n\n")

        if errors:
            response_message_parts.append(
                escape_markdown("*Notices/Errors during analysis:*", version=2) + "\n"
            )
            for err in errors:
                escaped_err = escape_markdown(str(err), version=2)
                response_message_parts.append(f"- `{escaped_err}`\n")

        response_message = "".join(response_message_parts).strip()

        max_length = 4096
        ellipsis_md = escape_markdown("...", version=2)
        truncation_msg_text = "_Message truncated due to length._"
        truncation_msg_md = escape_markdown(truncation_msg_text, version=2)

        if len(response_message) > max_length:
            safe_max_len_part1 = (
                max_length - len(truncation_msg_md) - len(ellipsis_md) - 5
            )

            part1 = (
                response_message[:safe_max_len_part1] if safe_max_len_part1 > 0 else ""
            )
            last_newline_idx = part1.rfind("\n")
            if last_newline_idx != -1:
                part1 = part1[:last_newline_idx]

            if not part1.strip() and response_message.strip():
                part1 = escape_markdown(response_message[:100], version=2)

            final_part1_text = part1
            if part1.strip():
                final_part1_text += f"\n{ellipsis_md}"
            elif response_message.strip():
                final_part1_text = ellipsis_md
            else:
                final_part1_text = ""

            if final_part1_text.strip():
                await update.message.reply_text(
                    final_part1_text, parse_mode=ParseMode.MARKDOWN_V2
                )
            await update.message.reply_text(
                truncation_msg_md, parse_mode=ParseMode.MARKDOWN_V2
            )
        elif response_message.strip():
            await update.message.reply_text(
                response_message, parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            await update.message.reply_text(
                escape_markdown(
                    "No information to display based on your query.", version=2
                ),
                parse_mode=ParseMode.MARKDOWN_V2,
            )

    except Exception as e:
        logger.error(
            f"Error during stats fetching or processing for addresses: {addresses_to_investigate}: {e}",
            exc_info=True,
        )
        error_message_text = f"An unexpected error occurred while processing your request. Please try again later or contact support."
        await update.message.reply_text(
            escape_markdown(error_message_text, version=2),
            parse_mode=ParseMode.MARKDOWN_V2,
        )

    finally:
        if "addresses_to_investigate" in context.user_data:
            del context.user_data["addresses_to_investigate"]
        return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    user = update.message.from_user
    logger.info("User %s canceled the conversation.", user.first_name)
    if "addresses_to_investigate" in context.user_data:
        del context.user_data["addresses_to_investigate"]

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
        # Optionally, you could prevent the bot from starting if RPCs are critical.

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Add conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("getstats", getstats_start)],
        states={
            ASK_ADDRESS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_address_received)
            ],
            ASK_EVENTS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_events_received)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,  # Ensures that we don't restart the conversation on every message if not expected
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)

    logger.info("Bot starting...")
    application.run_polling()


if __name__ == "__main__":
    main()
