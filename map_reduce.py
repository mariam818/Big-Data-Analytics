
import os
import re
import threading
from collections import defaultdict


def read_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


# Split text into words and emit (word, 1)
def mapper(file_path):
    text = read_file(file_path)
    words = re.findall(r"\b\w+\b", text.lower())

    result = []

    for word in words:
        result.append((word, 1))

    return result


def run_mappers(files):
    results = []
    lock = threading.Lock()

    def worker(file_path):
        local_result = mapper(file_path)

        with lock:
            results.extend(local_result)

    threads = []

    for file_path in files:
        thread = threading.Thread(target=worker, args=(file_path,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return results


def combine_and_partition(pairs, num_reducers=2):
    combined = defaultdict(int)

    for word, count in pairs:
        combined[word] += count

    partitions = [[] for _ in range(num_reducers)]

    for word, count in combined.items():
        reducer_id = hash(word) % num_reducers
        partitions[reducer_id].append((word, count))

    return partitions


def reducer(partition, output_file):
    partition.sort(key=lambda pair: pair[0])

    totals = defaultdict(int)

    for word, count in partition:
        totals[word] += count

    with open(output_file, "w", encoding="utf-8") as file:
        for word in sorted(totals):
            file.write(f"{word}\t{totals[word]}\n")


def main(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    files = [
        os.path.join(input_folder, name)
        for name in os.listdir(input_folder)
        if name.endswith(".txt") and name != "expected_word_count.txt"
    ]

    mapped_pairs = run_mappers(files)

    partitions = combine_and_partition(mapped_pairs)

    threads = []

    for i, partition in enumerate(partitions):
        output_file = os.path.join(output_folder, f"part-{i}.txt")

        thread = threading.Thread(
            target=reducer,
            args=(partition, output_file)
        )

        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("MapReduce completed successfully!")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    main(os.path.join(script_dir, "datasets", "text-files"), "output")
