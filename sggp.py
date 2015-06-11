import sys,threading
from optparse import OptionParser
from scribe import scribe
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol
from struct import *
import time
import signal
import sys
sys.stdout.flush()
sys.stderr.flush()
is_sigint_up = False
def sigint_handler(signum, frame):
	is_sigint_up = True
	print 'catched interrupt signal!'
def getInputParam():
	MSG_USAGE = '[-i<ip>] [-p<port>] [-d<data>] [-m<model>] [-s<size>] [-P<press>] [-h<help>]'
	optParser = OptionParser(MSG_USAGE)
	optParser.add_option("-i","--ip", action="store", type="string", dest="ip", help="target host")
	optParser.add_option("-p","--port", action="store", type="int", dest="port", help="target port" )
	optParser.add_option("-d","--data", action="store", type="string", dest="data", help="data source for pressure")
	optParser.add_option("-m","--model", action="store", type="string", dest="model", help="press model: loop or eof")
	optParser.add_option("-s","--tsize", action="store", type="int", dest="tsize", help="thread size")
	optParser.add_option("-P","--psize", action="store", type="int", dest="psize", help="qps")
	optParser.add_option("-g","--group", action="store", type="string", dest="group", default="",help="newline delimiter for data group")
	optParser.add_option("-c","--category", action="store", type="string", dest="category", help="data catagory")
	optParser.add_option("-l","--line", action="store", type="int", dest="lines",help="read n lines")

	options, args = optParser.parse_args()
	ret = {}
	ret['host'] = options.ip
	ret['port'] = options.port
	ret['data'] = options.data
	ret['lines'] = options.lines
	ret['model'] = options.model
	ret['tsize'] = options.tsize
	ret['psize'] = options.psize
	ret['group'] = options.group
	ret['category'] = options.category
	if None in ret.values():
		print optParser.print_help()
		return None		
	else:	
		return ret


class CWorker(threading.Thread):
	def __init__(self, cTPool, host, port, interval,category):
		threading.Thread.__init__(self)
		self.cTPool = cTPool
		self.host = host
		self.port = port
		self.interva = interval
		self.transport = None
		self.category = category
		self.send_count = 0

	def __initialize(self):
		socket = TSocket.TSocket(host=self.host, port=self.port)
		self.transport = TTransport.TFramedTransport(socket)
		protocol = TBinaryProtocol.TBinaryProtocol(trans=self.transport, strictRead=False, strictWrite=False)
		self.client = scribe.Client(iprot=protocol, oprot=protocol)
		self.transport.open()
	
	def __close(self):
		self.transport.close()
	def set_sendCount(self, count):
               	sys.stdout.flush()
		self.send_count = count
	def __sendData(self, data):
		messages = []
		for line in data.split("\n"):
			if line:
				log_entry = scribe.LogEntry(category=self.category, message=line)
				messages.append(log_entry)
		result = self.client.Log(messages)
		messages = None
        	if result == scribe.ResultCode.OK:
			self.send_count +=1
				
	def run(self):
		self.__initialize()
		global is_sigint_up
		while True:
			data = self.cTPool.getData()
			if data == None:
				break
			if is_sigint_up:
				break
			self.__sendData(data)
			time.sleep(self.interva)
		self.__close()

class CTPool(threading.Thread):
	def __init__(self, param):
		print 'init'
		threading.Thread.__init__(self)
		self.isInit = False
		self.param = param
		self.data = None
		self.mutex = None	
		self.wk_list = []
		self.press_size = 0
		self.__initialize()
	
	def __close(self):
		self.data.close()
	
	def __initialize(self):
		try:
			self.data = open(self.param['data'])
			self.isInit = True
			self.mutex = threading.Lock()
		except Exception,e:
			print e
	
	def __readGroup(self):
		delimiter = self.param['group']
		group=''
		while True:
			try:
				line = self.data.readline()
			except Exception,e:
				break
			if line == '':
				break
			elif delimiter in line:
				break
			else:
				group+=line
		return group
	# read  specific  lines
	def __readlines(self):
		count = 0
		lines = ''
		while True:
			try:
				line = self.data.readline()
				count+=1
			except Exception,e:
				break
			if line == '':
				break
			else:
				lines+=line
			if (count ==self.param['lines']):
				break
		return lines
				
								
	def __readLine(self):
		#group by group
		if self.param['group'] != "":
			return self.__readGroup()	
		if self.param['lines'] !="":
			return self.__readlines()
		#line by line
		try:
			line = self.data.readline()
			#print line
		except Exception,e:
			line = None
			print e	

		return line

	def getData(self):
		#self.press_size +=1
		self.mutex.acquire()
		try:
			data = self.__readLine()
			if data =='':
				if self.param['model']=='eof':
					data = None
				else:
					self.__close()
					self.data = open(self.param['data'])		
					data = self.__readLine()
		except Exception,e:
			data = None
			print e
		else:
			if data != None:
				pass
				#self.press_size +=1
		self.mutex.release()			
		return data
		
	def createWorker(self):
		host = self.param['host']
		port = self.param['port']
		tsize =  self.param['tsize']	
		psize = self.param['psize']	
		category = self.param['category']
		interval = tsize * 1.0 / psize
		for i in range(0, tsize):
			wk = CWorker(self, host, port, interval, category)							
			wk.start()
			self.wk_list.append(wk)

	def wait(self):
		while True:
			self.press_size = 0	
			time.sleep(1)
			i = 0
			for wk in self.wk_list:
				if not wk.isAlive():
					i+=1
				else:	
					self.press_size +=wk.send_count
					wk.set_sendCount(0)
			sys.stdout.write('QPS:%d\n' % self.press_size)
    			sys.stdout.flush()
			if i == len(self.wk_list):
                                break
							
	def run(self):
		if  not self.isInit:
			return
		self.createWorker()
		self.wait()	
		self.__close()		
 
def main():
	#signal.signal(signal.SIGINT, sigint_handler)		
	#signal.signal(signal.SIGTSTP, sigint_handler)		
	param = getInputParam()
	if param == None:
		return
	
	cTPool = CTPool(param)
	cTPool.start()	
	cTPool.join()

if __name__ == '__main__':
	sys.stdout.flush()
	sys.stderr.flush()
	main()	
