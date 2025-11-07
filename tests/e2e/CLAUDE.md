# CLAUDE.md

## End to end (e2e) integration tests

These tests are intended to be **end-to-end**.

1. **API-only interactions**

   * Do **not** manipulate the database directly.
   * All operations and verifications must be performed **through the API**.

2. **Test data setup**

   * It’s acceptable to create **test files in MinIO** before running the test.
   * However, all outcome validation must happen via **API responses**, not by inspecting the database.

3. **Single test focus**

   * Each time, work on **only one test case**.
   * Avoid handling multiple test methods at once — it makes reviews harder for users.

4. **Collaborative workflow**

   * When writing a new test or making significant changes, **first show the proposed changes** to the user.
   * **Discuss and confirm** the approach before applying modifications.
   * **Never make changes directly** without prior agreement.

5. **Use real systems**

   * Always use the real systems provided by **Docker Compose** — including **MinIO**, **PostgreSQL**, and **Spark**.
   * End-to-end tests must run against these real components to stay as close as possible to the **production-like environment**.

6. **Test execution**

   * Use **pytest** to run and verify that each test works correctly and passes end-to-end.