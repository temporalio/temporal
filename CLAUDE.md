- Before you make any changes, always switch to "plan mode" so that I can verify that any changes added is right.
- If writing tests, analyze the whole file before you write a test. Look for common helper methods that you can use so that you can not duplicate code. Focus on writing tests that work and on writing tests that are very easy to comprehend.
- while running tests in the tests/ directory, always run tests using a command like this :  go test -count=1 -v ./tests -tags test_dep -run TestVersioning3FunctionalSuite/TestUnpinnedWorkflow_Sticky
These are the tags that you will need.
- The versioning entity workflows (workflow.go) are async in nature. Always check the propagation status and ensure it shows that propagation is completed after any operation (registering workers, setting a version as current)
- Always verify if the test you have written is flaky or not. Do this by running the test with all the suites that are present in the file and by also re-running the test a few times.
- Feel free to always add in print statements to aid in debugging whenever you are working on tests and have to figure out why the test is failing.
- Always read through your scratchpad.MD to read the status of an ongoing project.
- While working on this project, note that we cannot use the signalwithstart API since we are assuming, and must, have the version workflow running even if it is in a drained state. This applies when you are developing the test.
moreover, remember that if you see that the signal is not being processed by the workflow, try finding out if the signal is being sent correctly and if the workflow is running at the time of sending the signal. Then, do some more debugging to find out why the signal is just not reaching the workflow.
- Rather than running the test 5 times, after introducing a fix, always run it once to see if it works or not. If it does, run it more times to see if the test is flaky or not.
- run the tes with the following command:
 go test -count=1 -v ./tests -tags test_dep -run TestDeploymentVersionSuiteV0/TestUpdateWorkflowExecutionOptions_ReactivateVersionOnPinned
- don't compare the namespace name and namespace ID - they are two different things; compare namespace name with namespace name and the same for id