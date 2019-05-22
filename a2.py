from flask import Flask, request
from flask_restplus import Resource, Api, fields, reqparse
import requests
import sqlite3
import random
import time

app = Flask(__name__)
api = Api(app,version='1.0',title='World Bank Economic Indicators API',
    description='A sample API')
indicator_type=api.model('indicators',{'indicator_id': fields.String})
parser = reqparse.RequestParser()
parser.add_argument('q',help = "LIKE: top<N> or bottom<N>")

def create_db(db_file):
	database_name = db_file+'.db'
	conn = sqlite3.connect(database_name)
	c = conn.cursor()
	c.execute("DROP TABLE IF EXISTS collections;")
	c.execute('CREATE TABLE collections (id INTEGER NOT NULL, collection_id INTEGER NOT NULL, indicator TEXT, indicator_value TEXT,\
		creation_time TEXT,country TEXT,dateT TEXT, value REAL,primary key(id,collection_id));')
	conn.commit()
	conn.close()

@api.route('/worldbank')
class collections(Resource):
	@api.response(200,'OK')
	@api.response(404,'error')
	@api.doc(description="Q3:Retrieve the list of available collections")
	def get(self):
		collections = 'worldbank'
		conn = sqlite3.connect('data.db')
		c = conn.cursor()
		id_set = set(c.execute("SELECT collection_id FROM collections"))
		if id_set == set():
			conn.commit()
			conn.close()
			return {'message':"the input doesn't exist in the data source"},404
		result = list()
		for e in id_set:
			line = list(c.execute("SELECT * FROM collections WHERE id = 1 and collection_id = '%d'" % e))
			print(line)
			result.append({"location" : "/{}/{}".format(collections,line[0][1]), "collection_id" : "{}".format(line[0][1]),\
				"creation_time": "{}".format(line[0][4]),"indicator" : "{}".format(line[0][2])})
		conn.commit()
		conn.close()
		return result,200
	
	@api.response(201,'Created')
	@api.response(200,'OK')
	@api.response(404,'error')
	@api.expect(indicator_type)
	@api.doc(description="Q1:Import a collection from the data service")
	def post(self):
		collections = 'worldbank'
		indicator_id = request.json['indicator_id']
		if len(requests.get("http://api.worldbank.org/v2/countries/all/indicators/{}?date=2013:2018&format=json".format(indicator_id)).json()) == 1:
			return {'message':"the input indicator id doesn't exist in the data source"},404
		total = requests.get("http://api.worldbank.org/v2/countries/all/indicators/{}?date=2013:2018&format=json".format(indicator_id)).json()[0]["total"]
		data = requests.get("http://api.worldbank.org/v2/countries/all/indicators/{}?date=2013:2018&format=json&per_page={}".format(indicator_id,total))
		data = data.json()[1]
		if data == []:
			return {'message':"The data source doesn't have any information about this"},404
		conn = sqlite3.connect('data.db')
		c = conn.cursor()
		indicator_set = c.execute("SELECT indicator FROM collections")
		if indicator_id in indicator_set:
			line = c.execute("SELECT * FROM collections WHERE id = 1 and indicator = '%s'" % indicator_id)
			result = {"location" : "/{}/{}".format(collections,line[0]), "collection_id" : "{}".format(line[0]),\
                              "creation_time": "{}".format(line[3]),"indicator" : "{}".format(line[1])}
			conn.commit()
			conn.close()
			return result,200

		id_set = c.execute("SELECT collection_id FROM collections")
		while(1):
			i = random.randint(1,20000)
			if i not in id_set:
				collection_id = i
				break
		creation_time = time.strftime("%Y-%m-%dT%H:%M:%SZ")
		i = 1
		for e in data:
			insert_data = [i,collection_id,e["indicator"]["id"],e["indicator"]["value"],creation_time,\
			               e["country"]["value"],e["date"],e["value"]]
			c.execute("INSERT INTO collections VALUES (?,?,?,?,?,?,?,?)", insert_data)
			i += 1
		result = {"location" : "/{}/{}".format(collections,collection_id),\
		          "collection_id" : "{}".format(collection_id),\
		          "creation_time": "{}".format(creation_time),\
		          "indicator" : "{}".format(indicator_id)}
		conn.commit()
		conn.close()
		return result,201

@api.route('/worldbank/<int:collection_id>')
@api.doc(params={'collection_id': 'input a id'})
class collections(Resource):
	#@api.response(201,'Created')
	@api.response(200,'OK')
	@api.response(404,'error')
	@api.doc(description="Q2:Deleting a collection with the data service")
	def delete(self,collection_id):
		collections = 'worldbank'
		conn = sqlite3.connect('data.db')
		c = conn.cursor()
		id_set = set(c.execute("SELECT collection_id FROM collections"))
		id_set = list(id_set)
		id_set = [e[0] for e in id_set ]
		if collection_id not in id_set:
			conn.commit()
			conn.close()
			return {'message':"the input Collection id doesn't exist in the data source"},404
		c.execute("DELETE FROM collections WHERE collection_id='%d'" % collection_id)
		result = {"message" :"Collection = {} is removed from the database!".format(collection_id)}
		conn.commit()
		conn.close()
		return result,200

	#@api.response(201,'Created')
	@api.response(200,'OK')
	@api.response(404,'error')
	@api.doc(description="Q4:Retrieve a collection")
	def get(self,collection_id):
		collections = 'worldbank'
		conn = sqlite3.connect('data.db')
		c = conn.cursor()
		line = list(c.execute("SELECT * FROM collections WHERE id = 1 and collection_id = '%d'" % collection_id))
		if line == []:
			conn.commit()
			conn.close()
			return {'message':"the input Collection id doesn't exist in the data source"},404
		result = {"location" : "/{}/{}".format(collections,line[0][1]), "collection_id" : "{}".format(line[0][1]),\
                      "creation_time": "{}".format(line[0][4]),"indicator" : "{}".format(line[0][2])}
		i = 1
		sub_result = list()
		while(1):
			line = list(c.execute("SELECT * FROM collections WHERE id = '%d' and collection_id = '%d'" % (i,collection_id)))
			if line == []:
				break
			entry = {"country":"{}".format(line[0][5]),"date":"{}".format(line[0][6]),"value":line[0][7]}
			sub_result.append(entry)
			i += 1
		result["entries"] = sub_result
		conn.commit()
		conn.close()
		return result,200

@api.route('/worldbank/<int:collection_id>/<string:year>/<string:country>')
@api.param("collection_id", 'input an ID')
@api.param('year','a year between 2013 and 2018')
@api.param('country','input a country name')
class collections(Resource):
	@api.response(200,'OK')
	@api.response(404,'error')
	@api.doc(description="Q5:Retrieve economic indicator walue for given country and a year")
	def get(self,collection_id,year,country):
		collections = 'worldbank'
		conn = sqlite3.connect('data.db')
		c = conn.cursor()
		line = list(c.execute("SELECT * FROM collections WHERE collection_id = '%d' and country = '%s' and dateT = '%s'" % (collection_id,country,year)))
		if line == []:
			conn.commit()
			conn.close()
			return {'message':"the input doesn't exist in the data source"},404
		conn.commit()
		conn.close()
		return {"collection_id":"{}".format(collection_id),"indicator" : "{}".format(line[0][2]),"country": "{}".format(country), "year": "{}".format(year),"value": line[0][7]}, 200

@api.route('/worldbank/<int:collection_id>/<year>')
@api.param("collection_id", 'input an ID')
@api.param('year','a year between 2013 and 2018')
class collections(Resource):
	@api.response(200,'OK')
	@api.response(404,'error')
	@api.expect(parser)
	@api.doc(description="Q6:Retrieve top/bottom economic indicator values for a given year")
	def get(self,collection_id,year):
		collections = 'worldbank'
		query = request.args.get('q')
		if query[:3] == 'top':
			order = 'value DESC'
			num = query[3:]
		elif query[:3] == 'bot':
			if query[:6] != 'bottom':
				return {'message':"the input is wrong"},404
			order = 'value ASC'
			num = query[6:]
		else:
			return {'message':"the input is wrong"},404
		if num == '':
			return {'message':"the input is wrong"},404
		else:
			num = int(num)

		if num not in range(1,101):
			return {'message':"the input is wrong"},404
		conn = sqlite3.connect('data.db')
		c = conn.cursor()
		lines = list(c.execute("SELECT * FROM collections WHERE collection_id = '%d' and value IS NOT NULL and dateT = '%s' ORDER BY %s LIMIT %d;" % (collection_id,year,order,num)))
		if lines == []:
			conn.commit()
			conn.close()
			return {'message':"the input is wrong"},404
		entry = list()
		for i in range(len(lines)):
			entry.append({"country":"{}".format(lines[i][5]),"date":"{}".format(lines[i][6]),"value":lines[i][7]})
		result = {"indicator":lines[0][2],"indicator_value":lines[0][3],"entries":entry}
		conn.commit()
		conn.close()
		return result,200

#if __name__ == '__main__':
	#create_db("data")
	#app.run(debug=True)
