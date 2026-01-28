# Terminal Games

A comprehensive development and deployment platform for TUI based games

## How does it work?

You can develop a terminal game in either Go with [bubbletea](https://github.com/charmbracelet/bubbletea) or Rust with [ratatui](https://ratatui.rs).
Then, you compile it to WASM and deploy to our global edge network of nodes running the `terminal-games` server.
Your game will be served via SSH to players at low latency across the world 🌎

## Features

- Low latency, global deployment
- Multiplayer peer messaging SDK
- Web support with [xterm.js](https://xtermjs.org)
- Audio (even over SSH!)
- Built in KV store 🚧
