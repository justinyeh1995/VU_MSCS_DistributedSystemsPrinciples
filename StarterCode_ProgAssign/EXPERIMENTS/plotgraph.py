import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import argparse
## pass a string from command line, indicating file name

parser = argparse.ArgumentParser ()
# add optional arguments
parser.add_argument ("-f", "--file", default="client2_tree_loss", help="file name")
args = parser.parse_args ()

f = args.file

df = pd.read_csv (f+'.csv', names=['latency'])
latency = df.latency.values
mean = np.full (100, np.mean(latency))
std = np.full (100, np.std(latency))
fig, ax = plt.subplots(figsize=[18,10])
degree = [d for d in range(1,101)]
ax.plot(degree, latency, c='b', marker="s", label='Data')
ax.plot(degree, mean, c='g', label='Mean: {:.2f}'.format(np.mean(latency)))
ax.plot(degree, std, c='r', label='Std: {:.2f}'.format(np.std(latency)))
plt.xticks(np.arange(1,101), degree, fontsize=5)
ax.set_xlabel('Degree')
ax.set_ylabel('Latency')
plt.title(f)
plt.legend()
plt.savefig(f+'.png')
plt.show()
