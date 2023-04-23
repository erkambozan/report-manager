from flask import Flask, request
import duckdb

app = Flask(__name__)

db = duckdb.connect()
db.execute("CREATE VIEW campaigns as SELECT * FROM 'campaigns.parquet'")
db.execute("CREATE VIEW keywords as SELECT * FROM 'keyword-perf.parquet'")


@app.route('/performance_by_day', methods=['GET'])
def performance_by_day():
    campaign_type = request.args.get('campaign_type')
    market_place = request.args.get('market_place')
    date = request.args.get('date')
    query = f'''SELECT *
                    FROM keywords
                    LEFT JOIN campaigns ON campaigns.campaignId = keywords.campaignId
                    WHERE 
                    name LIKE '%{campaign_type}%' AND
                    Marketplace LIKE '%{market_place}%' AND
                    startDate = '{date}'
                    LIMIT 1'''

    data = read_data_from_duck_db(query)
    response = parse_to_json(data)
    return response, 200


@app.route('/categorized_keywords_performance', methods=['GET'])
def categorized_keywords_performance():
    query = f'''SELECT * FROM keywords
                LEFT JOIN campaigns ON campaigns.campaignId = keywords.campaignId LIMIT 10'''
    data = read_data_from_duck_db(query)
    data = parse_to_json(data)
    response = categorization_campaign_type(data)
    return response, 200


def read_data_from_duck_db(query):
    return db.execute(query)


def parse_to_json(data):
    column_names = [column[0] for column in data.description]
    result = data.fetchall()
    return [dict(zip(column_names, row)) for row in result]


def categorization_campaign_type(data):
    for item in data:
        targeting = item['name'].split("_")[3].split("/")[0]
        if targeting != 'brand' and targeting != 'close' and targeting != 'cmp':
            targeting = 'non-brand'

        item['targeting'] = targeting
    return data


app.run(debug=True, port=5000, host='0.0.0.0')
