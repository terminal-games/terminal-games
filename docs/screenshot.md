From within tmux:

`ctrl+b :` `resize-window -x80 -x25`
`ctrl+b :` `capture-pane -e`
`ctrl+b :` `save-buffer /tmp/screenshot.ansi`

Then on command line:

`head -n -1 /tmp/screenshot.ansi | jq -Rs .`
