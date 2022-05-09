from mytelegram.raven import Raven

raven = Raven()

raven.send_all_clients("Restart TWS on EC2")
raven.stop()
