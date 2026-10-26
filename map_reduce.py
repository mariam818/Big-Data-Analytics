
import os
import re
import threading
from collections import defaultdict


NUM_REDUCERS = 2

def read_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def mapper(file_path):
    text = read_file(file_path)
    words = re.findall(r"\b\w+\b", text.lower())

    result = []

    for word in words:
        result.append((word, 1))

    return result


def run_mappers(files):
    results = [None] * len(files)
    threads = []

    def worker(index, file_path):
        results[index] = mapper(file_path)

    for index, file_path in enumerate(files):
        thread = threading.Thread(
            target=worker,
            args=(index, file_path)
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return results


def combine(mapper_results):
    combined_results = []

    for pairs in mapper_results:
        local_counts = defaultdict(int)

        for word, count in pairs:
            local_counts[word] += count

        combined_results.append(list(local_counts.items()))

    return combined_results


def partition(word):
    return sum(ord(char) for char in word) % NUM_REDUCERS


def partition_data(combined_results):
    partitions = [[] for _ in range(NUM_REDUCERS)]

    for mapper_pairs in combined_results:
        for word, count in mapper_pairs:
            reducer_id = partition(word)
            partitions[reducer_id].append((word, count))

    return partitions


def shuffle_and_sort(partition):
    grouped_words = defaultdict(list)

    for word, count in partition:
        grouped_words[word].append(count)

    sorted_words = {}

    for word in sorted(grouped_words):
        sorted_words[word] = grouped_words[word]

    return sorted_words


def reducer(grouped_words, output_file):
    with open(output_file, "w", encoding="utf-8") as file:
        for word, counts in grouped_words.items():
            total = sum(counts)
            file.write(f"{word}\t{total}\n")


def main(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    files = sorted([
        os.path.join(input_folder, name)
        for name in os.listdir(input_folder)
        if name.endswith(".txt")
        and name != "expected_word_count.txt"
    ])

    if not files:
        raise FileNotFoundError(
            f"No input text files found in {input_folder}"
        )

    print("Input splits:", len(files))
    mapper_results = run_mappers(files)
    print("Phase 1: Map completed")

    combined_results = combine(mapper_results)
    print("Phase 2: Combine completed")

    partitions = partition_data(combined_results)
    print("Phase 3: Partition completed")

    reducer_inputs = []

    for part in partitions:
        reducer_inputs.append(shuffle_and_sort(part))

    print("Phase 4: Shuffle and Sort completed")

    threads = []

    for i, grouped_words in enumerate(reducer_inputs):
        output_file = os.path.join(
            output_folder, f"part-{i}.txt"
        )

        thread = threading.Thread(
            target=reducer,
            args=(grouped_words, output_file)
        )

        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("Phase 5: Reduce completed")
    print("MapReduce completed successfully!")
    print("Output files: part-0.txt, part-1.txt")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))

    input_folder = os.path.join(
        script_dir, "datasets", "text-files"
    )

    output_folder = os.path.join(script_dir, "output")

    main(input_folder, output_folder)
