import OpenOPC
import threading
import time
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt

def pubService():
	opc = OpenOPC.client()
	opc.connect('Matrikon.OPC.Simulation.1')
	while True:
		opcData = opc.read(opc.list('Configured Aliases.testEmir'))
		msgs=[]
		for i in opcData:
			alias,item = i[0].split(".");
			msgs.append(("/plcNetwork/dev0/get/"+alias+"/"+item+"/", i[1], 0, False));

		publish.multiple(msgs, hostname="###")
		time.sleep(0.5)
	opc.close()

	
def subService():
	opc = OpenOPC.client()
	opc.connect('Matrikon.OPC.Simulation.1')
	def on_connect(client, userdata, flags, rc):
		print("Connected with result code "+str(rc))
		client.subscribe("/plcNetwork/dev0/set/#")

	def on_message(client, userdata, msg):
		topicParse=msg.topic.split("/")
		opc.write( (topicParse[-3:][0]+"."+topicParse[-3:][1], msg.payload) )
		print "set: "+topicParse[-3:][0]+"."+topicParse[-3:][1]+": "+msg.payload

	client = mqtt.Client()
	client.on_connect = on_connect
	client.on_message = on_message

	client.connect("###", 1883, 60)
	client.loop_forever()
	opc.close()

threading.Thread(name='pubService', target=pubService).start()
threading.Thread(name='subService', target=subService).start()