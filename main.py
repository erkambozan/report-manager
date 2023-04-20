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

    response = read_data_from_duck_db(campaign_type, market_place, date)

    return response, 200


def read_data_from_duck_db(campaign_type, market_place, date):
    query = f'''SELECT * FROM keywords
                LEFT JOIN campaigns ON campaigns.campaignId = keywords.campaignId
                WHERE 
                name LIKE '%{campaign_type}%' AND
                Marketplace LIKE '%{market_place}%' AND
                startDate = '{date}'
                LIMIT 1
                '''

    cursor = db.execute(query)
    column_names = [column[0] for column in cursor.description]
    result = cursor.fetchall()
    json_obj = [dict(zip(column_names, row)) for row in result]

    return json_obj

app.run(debug=True, port=5000, host='0.0.0.0')
