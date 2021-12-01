import os
import datetime

DATA_DIR = 'data_old'
data = {
    'time': [],
    'polarity': [],
    'subjectivity': [],
}
for fn in os.listdir(DATA_DIR):
    if fn[-4:] != '.csv':
        continue
    fpath = os.path.join(DATA_DIR, fn)
    time = datetime.datetime.fromtimestamp(os.stat(fpath).st_mtime)
    with open(fpath, 'r') as f:
        lines = f.readlines()
    if len(lines) == 0:
        continue
    subjectivities = []
    polarities = []
    for line in lines:
        cols = line.split(',')
        if len(cols) > 3:
            tweet = ''.join(cols[0:-2])
        else:
            tweet = cols[0]
        subjectivities.append(float(cols[-2]))
        polarities.append(float(cols[-1]))
    av_pol = sum(polarities)/len(polarities)
    av_sub = sum(subjectivities)/len(subjectivities)
    data['time'].append(time)
    data['polarity'].append(av_pol)
    data['subjectivity'].append(av_sub)

