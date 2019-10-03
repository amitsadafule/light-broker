import broker
from os import walk

b = broker.Broker()
b.start()

print('starting producer...')

for (dirpath, dirnames, filenames) in walk('./input'):
	for fileName in filenames:
		with open('./input/'+fileName, 'rb') as f:
			b.push(f.read(), 'name='+fileName, isByteData=True)