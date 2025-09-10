# n-Gram Counter (Hadoop MapReduce)

This project implements an **n-gram counter** using Hadoop MapReduce in Java.  
It processes a text dataset, generates **n-grams** (sequences of `n` consecutive words), and counts their frequencies.

---

## 📂 Project Structure

├── src/
│ └── nGrams/
│ └── nGramsCounter.java # Main MapReduce job
├── input/
│ └── text.txt # Input dataset (plain text file)
└── output/ # Output folder (generated after job completion)


---

## ⚙️ Requirements

- **Java JDK 8+**
- **Apache Hadoop** (2.x or 3.x)
- Linux/MacOS/WSL environment recommended

---

## ▶️ How to Compile & Run

1. **Compile and package into JAR**:
   ```bash
   javac -classpath `hadoop classpath` -d classes src/nGrams/nGramsCounter.java
   jar -cvf ngrams.jar -C classes/ .

Run with Hadoop:

hadoop jar ngrams.jar nGrams.nGramsCounter input/text.txt output 3

input/text.txt → input dataset

output → output directory (must not exist before running)

3 → size of the n-grams (in this case, trigrams)

View results:
hdfs dfs -cat output/part-r-00000

📊 Example Input
Hadoop is an open source framework.
Hadoop makes big data processing easier.

📊 Example Output (bigrams with n=2)
hadoop is     1
is an         1
an open       1
open source   1
source framework 1
hadoop makes  1
makes big     1
big data      1
data processing 1
processing easier 1

--------------------------------------------------
📝 How It Works

Mapper

Converts text to lowercase and removes punctuation.

Splits text into words.

Generates n-grams (sliding window of n words).

Emits each n-gram as key with value 1.

Reducer

Sums the counts for each n-gram.

Outputs ngram → frequency.
