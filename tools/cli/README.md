Documentation for the Temporal command line interface is located at our [main site](https://docs.temporal.io/docs/08_cli).

## Quick Start
Run `make bins` from the project root. You should see an executable file called `tctl`. Try a few example commands to 
get started:   
`./tctl` for help on top level commands and global options   
`./tctl domain` for help on domain operations  
`./tctl workflow` for help on workflow operations  
`./tctl tasklist` for help on tasklist operations  
(`./tctl help`, `./tctl help [domain|workflow]` will also print help messages)

**Note:** Make sure you have a Temporal server running before using the CLI.
