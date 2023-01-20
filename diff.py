import csv

files = ["output.csv", "output_backup.csv"]

outputs = [{}, {}]

for i, file in enumerate(files):
    with open(file) as f:
        reader = csv.reader(f)
        for row in reader:
            house_num, street, borough, *rest = row
            outputs[i][(house_num, street)] = rest[-1]


[one, two] = outputs
for key, value in one.items():
    (house_num, street) = key
    try:
        two_value = two[(house_num, street)]
        print("match" if value == two_value else "NO MATCH")
    except KeyError:
        pass
