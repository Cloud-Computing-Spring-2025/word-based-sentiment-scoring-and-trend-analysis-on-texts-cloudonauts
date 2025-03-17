import pandas as pd
import matplotlib.pyplot as plt

# Load Task 4 output file
df = pd.read_csv("output_task4/part-r-00000", sep="\t", header=None)

# Handling different row formats: Some rows have (bookID, decade), others just decade
df[0] = df[0].astype(str)

# Separate book-level trends and overall decade trends
df_overall = df[~df[0].str.startswith("(")].copy()  # Explicit copy to avoid warning
df_books = df[df[0].str.startswith("(")].copy()  # Explicit copy

# Rename columns based on structure
df_overall.columns = ["Decade", "SentimentScore", "Count", "Sum", "FinalScore"]

# Fix the SettingWithCopyWarning using .loc
df_overall.loc[:, "Decade"] = df_overall["Decade"].str.replace("s", "", regex=False).astype(int)

# Sort by decade
df_overall = df_overall.sort_values(by="Decade")

# Plot sentiment trend over decades
plt.figure(figsize=(10, 5))
plt.plot(df_overall["Decade"], df_overall["FinalScore"], marker='o', linestyle='-', label="Sentiment Score")

# Customizations
plt.xlabel("Decade")
plt.ylabel("Total Sentiment Score")
plt.title("Sentiment Trends Over Decades")
plt.grid()
plt.legend()

# Show the graph
plt.show()
