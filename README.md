# L-store HTAP Database System

## 🚀 Overview

The **L-store** is a **Hybrid Transactional/Analytical Processing (HTAP)** database system designed to integrate both **transactional** and **analytical** processing capabilities into a unified platform. It efficiently manages and manipulates data, handling both **OLTP (Online Transaction Processing)** and **OLAP (Online Analytical Processing)** workloads simultaneously. This system enables real-time data analysis while maintaining high-performance transactional processing.

---

## 🔧 Features

- **🔄 Hybrid Transactional and Analytical Processing (HTAP):** Seamlessly handles both transactional and analytical workloads in parallel.
- **⚡ Contention-Free Merge Process:** Ensures efficient record updates and eliminates conflicts during concurrent transactions.
- **📂 Page Range Management:** Optimized for handling large data sets through efficient page management and range-based queries.
- **🔒 Locking and Latching Mechanisms:** Implements a sophisticated locking mechanism for managing concurrent access to records, ensuring data integrity.
- **⚙️ Efficient Record Updates:** Supports both transactional updates and analytical queries without performance degradation.

---

## 🧩 Components

### 1. **📊 Database System (L-store)**
The L-store is the heart of the system, combining a database engine capable of processing both transactional and analytical workloads. It integrates features like:

- **📄 Page Management**: Optimized for large data sets, ensuring efficient storage and retrieval.
- **⚡ Contention-Free Merge**: A key feature to update records without conflicts in multi-user environments.
- **🔒 Efficient Locking**: Exclusive and shared locks, as well as latches, to handle concurrent operations safely.

### 2. **💼 Transaction Processing System**
The transaction processing system is responsible for executing, committing, or aborting transactions. It interacts with the database to manage transactional operations efficiently. Key functionalities include:

- **✅ Transaction Commit and Abort**: Supports commit and abort functionality for transactions, ensuring consistency and rollback in case of failure.
- **🔑 Lock Management**: Manages exclusive and shared locks to handle concurrent transaction access.
- **💻 Threaded Execution**: Uses multithreading to process transactions concurrently, improving system throughput.

### 3. **🔄 Merge Mechanism**
L-store employs a **contention-free merge** process to handle updates to records efficiently, even in multi-user environments. This process ensures that records are updated in a way that avoids conflicts and maintains data integrity across all transactions.

### 4. **🔗 HTAP Integration**
The system is designed to perform both **OLTP** and **OLAP** tasks concurrently. This integration allows for real-time data analytics without sacrificing the performance of transactional operations.

---

## 🛠 Technologies Used

- **🐍 Python**: Core programming language for implementing the database and transaction processing system.
- **🧵 Threading**: Utilized for handling concurrent transactions and operations, improving system performance and responsiveness.
- **🔒 Locking Mechanisms**: Implemented using custom lock and latch classes to ensure data integrity during concurrent access.
- **📄 Page Management**: Custom-designed system for efficient storage and retrieval of large datasets.

---

## 🔍 How It Works

### 💼 Transaction Processing

1. **📝 Transaction Creation**: Transactions are added to the system, which consist of queries that interact with database records.
2. **🔒 Locking**: Before executing any query, the system acquires locks (exclusive or shared) on the records being modified.
3. **✅ Commit or Abort**: After executing the queries, the transaction either commits, saving the changes, or aborts, rolling back the modifications.
4. **⚙️ Concurrency**: The system allows multiple transactions to run concurrently, ensuring that all transactions are processed without conflicts using locking mechanisms.

### 🔄 Merge Process

L-store's **contention-free merge** process ensures that updates to records are handled without conflicts. When multiple transactions try to update the same record, the merge process efficiently handles these updates, ensuring data consistency and eliminating race conditions.

---

## 🏗 Installation

### 📋 Requirements

- Python 3.x
- Threading module (standard in Python)

### ⚙️ Steps to Run

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/L-store.git
    ```
2. Navigate to the project directory:
    ```bash
    cd L-store
    ```
3. Run the system (example):
    ```bash
    python main.py
    ```

---

## 📝 Usage

Once the system is running, you can simulate transactions and database queries. You can test various aspects of the L-store HTAP system, including:

- **💼 Transaction Processing**: Create, commit, and abort transactions.
- **📊 Query Execution**: Perform analytical queries alongside transactional updates.
- **🔒 Lock Management**: Test the system's ability to handle concurrent access to records.

---

## 🤝 Contributing

Feel free to fork this repository, submit issues, or contribute improvements to the system. All contributions are welcome.
