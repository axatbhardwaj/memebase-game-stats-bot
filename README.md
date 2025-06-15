# Memebase Game Stats Bot

## Overview
A Telegram bot designed to provide on-chain event statistics for the Memebase contract on the Base network. It helps users track various game-related events for specified Ethereum addresses.

## Features
- **On-chain Event Analysis**: Fetches and analyzes events such as Hearted, Collected, Summoned, Unleashed, and Purged from the Memebase contract.
- **Multi-address Support**: Users can query statistics for one or multiple Ethereum addresses.
- **Address Validation**: Automatically validates provided Ethereum addresses.
- **ETH to USD Conversion**: Displays transaction amounts in both ETH and USD (where applicable).
- **Robust Data Fetching**: Utilizes multiple RPC URLs for reliable and efficient data retrieval.
- **Recent Data**: Focuses on event data from approximately the last 7 days of blockchain activity.

## Bot Usage

Interact with the bot on Telegram:

- `/start`: Displays a welcome message and introduction to the bot.
- `/getstats`: Initiates a conversation to gather Ethereum addresses and event types for analysis.
    - You will be prompted to enter one or more Ethereum addresses (comma-separated).
    - Then, you can select which events you want to analyze (e.g., "All Events", "Hearted", "Collected").
- `/cancel`: Stops the current conversation with the bot.
