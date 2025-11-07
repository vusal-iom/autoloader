Use @docs/scheduler/PREFECT_MIGRATION_GUIDE.md implementing the next feature. 
Please check the current implementation to understand what has been done so far. 
Pick next potential deliverable from there. First explain your plan for implementing it in a concise manner. 
Once we are good implement the test and the feature. Deliverable should be test and feature implementation.



Use @docs/scheduler/PREFECT_MIGRATION_GUIDE.md implementing the next deliverable. Other documents about migration is in the "docs/prefect-migration" folder 
Please check the current implementation to understand what has been done so far. Deliverable should be compact and focused on the next step.First explain your plan for implementing it in a concise manner. Note: This is not production code yet, no need to be backward compatible

please read all the docs and docs folder and figure out what would be potential high level items we need to work. Cross check code base also to see what is 
implemented/not implemented



Can you migrate @test_prefect_flow.py to e2e package and test simple flow. You can see @tests/e2e/test_basic_ingestion.py as a 
reference. I want similar system. Prefect run end to end, with json files uploaded, then prefect runs, then we verify the result. 
Using real systems, minio, postgresql, spark connect as you see in other e2e tests too. Implement 1 basic ingestion test using this 
prefect. Make sure you run and verify it works 
