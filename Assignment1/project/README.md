# Distributed Fragmented Database (NoSQL Assignment)

## Overview
This project implements a **horizontally fragmented distributed database** using PostgreSQL.  
Student and grade data are partitioned across multiple database fragments, and queries are routed and aggregated so that the final output matches a single-instance baseline.

The goal is to simulate core NoSQL concepts on top of a relational system:
- sharding
- deterministic routing
- distributed query aggregation
- idempotent writes

---

## Architecture

**Fragments**
frag0
frag1
frag2


**Partition key:** `student_id`

Routing function:
fragment = hash(student_id) % 3


Each student and their grades are stored in the same fragment.

---

## Project Structure
project/
├─ src/main/java/
│ ├─ Driver.java
│ └─ fragment/
│ ├─ FragmentClient.java
│ └─ Router.java
├─ src/main/resources/
│ └─ workload.txt
├─ scripts.sql
└─ output.txt


---

## Setup

### 1. Create fragment databases
```sql
CREATE DATABASE frag0;
CREATE DATABASE frag1;
CREATE DATABASE frag2;
```
2. Initialize schema

Run scripts.sql on each fragment:
```
psql -U postgres -d frag0 -f scripts.sql
psql -U postgres -d frag1 -f scripts.sql
psql -U postgres -d frag2 -f scripts.sql
```

3. Build
```
mvn clean compile
mvn dependency:copy-dependencies
```

4. Run
```
java -cp "target/classes;target/dependency/postgresql-42.7.3.jar;target/dependency/*" Driver
```

This generates output.txt

# Idempotent Inserts

The workload contains repeated insert operations.
To avoid failures, inserts are handled using:
ON CONFLIT DO NOTHING
This makes execution safe even if duplicate requests occur.

# Generating Expected Output
1. Create a single database
   baseline
2. Run the same workload with:
   NUM_FRAGMENTS = 1
3. Save the generated output as:
   expected_output.txt

# Accuracy Evaluation
## Outputs are compares line-by-line

```
accuracy = (total_lines - differing_lines) / total_lines) x 100
```

This distributed system output matches the baseline.
Conclusion

The system successfully demonstrates deterministic sharding, distributed query execution, and aggregation while producing results equivalent to a centralized database.
