# Umpire sparse regressions: remaining work

- [x] Validate completed suites in `Compile` before returning them.
  - Call `ValidateSuite` after constructing the completed `Suite`.
  - Return a structured compilation error if validation fails.
  - Test that every successfully compiled suite passes `ValidateSuite`.

- [ ] Report missing realizers and resources as structured compilation errors.
  - Validate selected action, policy, and resource realizations before execution.
  - Validate action and policy resource references and resource dependency chains.
  - Include the source instruction and a stable error category in each failure.
  - Test missing realizations, missing resources, and resource dependency cycles.
