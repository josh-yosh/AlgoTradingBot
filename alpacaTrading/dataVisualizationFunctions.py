import pandas as pd
import matplotlib.pyplot as plt

def makeGraph(fileName):
    df = pd.read_csv(fileName)

    df["date"] = df["timestamp"].str[:10]


    # Plot using pandas' built-in wrapper around matplotlib
    df.plot(x="date", y="close", title="Stock Prices Over Time")
   

    # Show the plot
    plt.show()

makeGraph("HistoricalData/SMCI_2025_7_1.csv")