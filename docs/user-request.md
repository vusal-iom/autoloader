### Ingestion

For the jobs I’m writing, I import data from AWS S3 into our data lake. I have AWS, Snyk just dump their output there for now.

Right now, I write custom Spark code to list, fetch, and import these. This is quite a bit of work to properly test and even debug Spark.

What I’d really like is **a simple scheduled ingestion**: check an S3 bucket, Azure Blob, GCS, or FTP hourly, daily, or on whatever schedule, and automatically load new files into a table — no Spark code or extra effort from the user.

As Shafi mentioned: making this easy for such a common use case could boost usage in existing customers.

Fuad did point out we have a **marketplace streaming Job** for this, but this would be up and running all the time — while most of my files come in daily or less frequently — so a scheduled approach would save resources and simplify things.

Even then, the job makes the UX clunkier than just a simple UI to configure a file ingestion (which underneath can be a job).