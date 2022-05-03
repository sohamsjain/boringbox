from backtrader.stores import IBStore

from mytelegram.raven import Raven

raven = Raven()


def listener(message):
    global store, raven
    print(message)
    if message.errorCode == 502:
        raven.send_all_clients("Restart TWS on EC2")


store = IBStore(host="52.70.61.124", port=7497, _debug=False)
store.conn.registerAll(listener)
store.start()
