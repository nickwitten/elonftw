from flask import Flask, jsonify, abort, request, make_response, url_for, session
from flask import render_template, redirect, Markup
from stockstream import *
import json
import datetime
import pandas as pd
import os
import datetime
app = Flask(__name__, static_url_path="")


def parse_twitter_data():
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


@app.route('/')
def hello():
    stockData = getCloseData()
    stockData = stockData.to_dict()
    prices = []
    times =[]
    for key, val in stockData.items():
        times.append(str(key))
        prices.append(val)
    finalData = {'index': times, 'data': prices}
    currentPrice = finalData['data'][len(finalData['data']) -1]
    return render_template('index.html', stockData = json.dumps(finalData), finalValue = currentPrice)


@app.errorhandler(400)
def bad_request(error):
    """ 400 page route.

    get:
        description: Endpoint to return a bad request 400 page.
        responses: Returns 400 object.
    """
    return make_response(jsonify({'error': 'Bad request'}), 400)


@app.errorhandler(404)
def not_found(error):
    """ 404 page route.

    get:
        description: Endpoint to return a not found 404 page.
        responses: Returns 404 object.
    """
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5001)
