import json
import numpy as np


with open('movie.json', encoding='utf-8', mode='r') as ff:
    jsdata = json.load(ff)

ids = np.array_split(range(len(jsdata['movie'])), 8)
for i, idx in enumerate(ids):
    jsdata_part = [jsdata['movie'][x] for x in idx]
    jsdata_part = {'movie':jsdata_part}

    with open(f'movie_part{i+1}.json', 'w') as ff:
        json.dump(jsdata_part, ff, indent=4)