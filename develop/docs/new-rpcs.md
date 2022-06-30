## Adding new RPCs

Adding new RPCs to any of the server roles is currently a little involved.
Here's a list of what you'll need to modify:

1. Add RPC definitions to proto files. Either in the `api` repo (for frontend)
   or here in `proto/internal` (for history/matching).
2. Update `go.temporal.io/api` to pick up the new generated files (for frontend)
   or generate them here with `make proto` (for history/matching).
3. Add metric scope defs for client-side metrics to `common/metrics/defs.go`
   (this is going away soon).
4. `make go-generate` to generate wrappers in `client`.
5. For frontend: add definitions to `service/frontend/dcRedirectionHandler.go`.
   (In the future hopefully we can make this generated or use interceptors.)
6. Add your new methods to `service/<service>/configs/quotas.go`.
7. Finally implement your new methods in the RPC handler.

