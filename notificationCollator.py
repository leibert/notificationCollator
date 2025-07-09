#!/opt/notificationCollator/env/bin/python3

# You need https://github.com/AsamK/signal-cli installed and working and
# paired up to your phone before any of this can be used.
#
# 1) Start signal-cli in dbus daemon mode: signal-cli -u <+phone> daemon
# 2) Start this thing.
# 3) Connect to localhost:60667 with an IRC client.



#notes for matrix bot
#https://matrix.org/docs/guides/creating-a-simple-read-only-matrix-client


import sys
import datetime
import pickle


from datetime import datetime
from gi.repository import GLib
# from pydbus import SystemBus
from pydbus import SystemBus   # for DBus processing
# from gi.repository import GLib  # for DBus processing

import asyncws
import asyncio
import json
import paho.mqtt.client as mqtt 
import sys
import time
import requests
import logging
from dotenv import load_dotenv
import os 

load_dotenv()

#setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# this is just to make the output look nice
formatter = logging.Formatter(fmt="%(asctime)s %(name)s.%(levelname)s: %(message)s", datefmt="%Y.%m.%d %H:%M:%S")

# this logs to stdout and I think it is flushed immediately
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)

#configure home assistant connection

hass_token = os.environ.get('HASS_TOKEN') # fill in your token
hass_host = os.environ.get('HASS_HOST') # get from env if set
hass_port= os.environ.get('HASS_PORT', 8123) # get from env if set

cal_scraper_host = os.environ.get('CAL_SCRAPER_HOST')

entities = [
    "sensor.sm_s901u1_last_notification",
    "sensor.sm_s901u1_active_notification_count",
    "sensor.sm_s901u1_last_removed_notification"
]

#cache for message backlog
messageQueue = []
mqttSignMode = "bigClock"





#SIGNAL STUFF

try:
    # load nick map from pickle file
    with open('nickPickle.dat', 'rb+') as filehandler:
        signal_nick_map = pickle.load(filehandler)
except EOFError:
    signal_nick_map = {}
except:
    signal_nick_map={}

#recieve messages from signal
def receiveSignalMessage(timestamp, source, group_id, message, attachments):
    print("sig msg")
    # logger.info("signal message recieved")
    try:
        print(f"{datetime.now()}: Message from {source}: {message}")
        # (f"{datetime.now()}: Signal Message from {source}: {message}")
        # print(group_id)
        # if group_id:
            # print(signal.getGroupName(group_id))

        # set a flag to see if the nickmap is updated by a new message
        nickMapUpdated = False

        sendingUserName = signal.getContactName(source)

        if sendingUserName:
            fromnick = sendingUserName.replace(' ', '_').replace(':', '')
            if not fromnick in signal_nick_map:
                signal_nick_map[fromnick] = source
                nickMapUpdated = True
        else:
            fromnick = source

        if group_id:
            print("message came from group")
            print(f"groups from {group_id}:")
            groupName = signal.getGroupName(group_id)
            groupName = 'GRP_' + groupName.replace(' ', '_').replace(':', '')
            if not groupName in signal_nick_map:
                signal_nick_map[groupName] = group_id
                nickMapUpdated = True
            message = fromnick + "- " + message
            senderName = groupName
        elif fromnick:
            senderName = fromnick
        else:
            senderName = source


        if attachments:
            print("attachments are present")


        # handle nick map updates
        if nickMapUpdated:

            with open('nickPickle.dat', 'wb+') as filehandler:
                pickle.dump(signal_nick_map, filehandler)
        
        #replace _ with spaces
        senderName=senderName.replace("_"," ")

        messageQueue.append((datetime.now(),"Signal",senderName,message))
        
    except Exception as e:
        print (e.message, e.args)

    # return False

###recieve msgs from home assistant
#recieve messages from home assistant
async def monitorHASSSocket():
    try:
        # websocket = await asyncws.connect('ws://{}:{}/api/websocket'.format(host, port))
        websocket = await asyncws.connect('ws://{}:{}/api/websocket'.format(hass_host, hass_port))


        await websocket.send(json.dumps({'type': 'auth','access_token': hass_token}))
        await websocket.send(json.dumps({'id': 1, 'type': 'subscribe_events', 'event_type': 'state_changed'}))
    except:
        logger.info("wsStateONCONNECT")
        await asyncio.sleep(300)
        logger.debug("Assume websocket fail, wait a longer time and try again")
        # time.sleep(5)
        print("WEBSOCKET FAIL.... attempting restart")
        sys.stdout.flush()
        os.execv(sys.executable, [sys.executable, __file__] + [sys.argv[0]])
        # sys.exit()
    
    print("Start socket...")
    logger.info("started HASS SOCKET")
    # messageQueue.append((datetime.now(),"Test msg","title","THIS IS A REALLY REALLYreally long test message"))

    while True:
        message = await websocket.recv()
        if message is None:
            logger.info("wsState")
            await asyncio.sleep(10)
            logger.debug("Assume websocket fail, exit and let systemd restart")
            sys.exit()
            # logger.debug("argv was",sys.argv)
            # logger.debug("sys.executable was", sys.executable)
            # logger.debug("restart now")
            # os.execv(sys.executable, ['python3'] + sys.argv)

            # logger.info(websocket.connected)

            continue

        # logger.info("msgRCV")
        # logger.info(message)
        if "event" not in message:
            continue
        
        try:   
            data = json.loads(message)['event']['data']
            # print(json.loads(message)['event']['data'])
            # print(data['entity_id'])
            entity_id = data['entity_id']
            
            if entity_id in entities:
                
                print("recieved {} ".format(entity_id))
                
                # if 'unit_of_measurement' in data['new_state']['attributes']:
                #     cache[entity_id] = "{} {}".format(data['new_state']['state'], data['new_state']['attributes']['unit_of_measurement'])
                # else:
                #     # cache[entity_id] = data['new_state']['state']
                #     cache[entity_id] = "{} {}".format(data['new_state']['state'], data['new_state']['attributes'])
                

                #parse signal
                # data['new_state']
                if "new_state" not in data or "attributes" not in data['new_state'] or "package" not in data['new_state']['attributes']:
                    logger.info("ERR182 key missing")
                    logger.info(data)
                    
                    continue

                if data['new_state']['attributes']['package'] == "org.thoughtcrime.securesms":
                    print("Signal Message detected..old handler")
                    logger.info("Signal Message detected..old handler")

                    continue
                    signalMessage=data['new_state']['attributes']
                    
                    #filter out status notifications
                    if (data['new_state']['attributes']['android.title']=='Signal' and data['new_state']['attributes']['android.text']=='Checking for messages…'):
                        break

                    print("message from:"+data['new_state']['attributes']['android.title'])
                    print("message text:"+data['new_state']['attributes']['android.text'])
                    # client.publish("ledSign/mode", "message")
                    # client.publish("ledSign/message/type", "signal")
                    # client.publish("ledSign/message/sender", data['new_state']['attributes']['android.title'])
                    # client.publish("ledSign/message/text", data['new_state']['attributes']['android.text'])
                    messageQueue.append((datetime.now(),"Signal",data['new_state']['attributes']['android.title'],data['new_state']['attributes']['android.text']))
                    
                    

                # Grab text messages
                elif data['new_state']['attributes']['package'] == "com.google.android.apps.messaging":
                    print("Text Message detected")
                    # textMessage=data['new_state']['attributes']['android.text'][0].split("  ")
                    #get message text
                    if 'android.messages' in data['new_state']['attributes']:
                        textMessage = (data['new_state']['attributes']['android.messages'].pop()).split(", ")
                        sender = textMessage[2][7:]
                        messageContent = textMessage[3][5:]
                    
                    if 'android.textLines' in data['new_state']['attributes']:
                        print ("queue of text")
                        #get last msg
                        # textMessage = data['new_state']['attributes']['android.textLines'][len(data['new_state']['attributes']['android.textLines'])-1]
                        textMessage = data['new_state']['attributes']['android.textLines'][0]
                        textMessage = textMessage.split("  ")
                        sender = textMessage[0]
                        messageContent = textMessage[1]

                    print("message from:"+sender)
                    print("message text:"+messageContent)
                    logger.info("message from:"+sender)
                    logger.info("message text:"+messageContent)
                    
                    messageQueue.append((datetime.now(),"SMS",sender,messageContent))
                
                # Grab Outlook notifications
                elif data['new_state']['attributes']['package'] == "com.microsoft.office.outlook":
                    print("Outlook Message detected")
                    # textMessage=data['new_state']['attributes']['android.text'][0].split("  ")
                    # textMessage = (data['new_state']['attributes']['android.messages'].pop()).split(", ")
                    # print("message from:"+textMessage[1])
                    # print("message text:"+textMessage[2])
                    # messageQueue.append((datetime.now(),"IBM",data['new_state']['attributes']['android.title'],data['new_state']['attributes']['android.title']))

                #grab fastmail notification
                elif data['new_state']['attributes']['package'] == "com.fastmail.app":
                    print("Fastmail Message detected")
                    print(data['new_state']['attributes'])

                    # textMessage=data['new_state']['attributes']['android.text'][0].split("  ")
                    # textMessage = (data['new_state']['attributes']['android.messages'].pop()).split(", ")
                    # print("message from:"+textMessage[1])
                    # print("message text:"+textMessage[2])
                    messageQueue.append((datetime.now(),"Fastmail",data['new_state']['attributes']['android.title'],data['new_state']['attributes']['android.title']))
                
                #get calendar notifications
                elif data['new_state']['attributes']['package'] == "com.samsung.android.calendar":
                    print("CALENDAR EVENT")
                    print(data['new_state']['attributes'])
                    # textMessage=data['new_state']['attributes']['android.text'][0].split("  ")
                    # textMessage = (data['new_state']['attributes']['android.messages'].pop()).split(", ")
                    # print("message from:"+textMessage[1])
                    # print("message text:"+textMessage[2])
                    # messageQueue.append(datetime.now(),"Fastmail",data['new_state']['attributes']['android.title'],data['new_state']['attributes']['android.title'])



                else:
                    logger.info("notification from "+data['new_state']['attributes']['package'])
                    print("notification from "+data['new_state']['attributes']['package'])





        except KeyError as e:
            logger.info("error35")
            logger.info("key error")
            logger.info(e)
            continue     
        except Exception as e:
            logger.info("error3")
            logger.info(e)
            print (e)
            continue



#############
#send messages to LED SIGN

async def initMessageSender():
    global mqttSignMode
    print("Start msg sender...")
    logger.info("started msg sender")

    

    await asyncio.sleep(.01)
    activeQueue=False
    
    while True:
        # print("messageSender")    
        if messageQueue == None or len(messageQueue) == 0:
              #if this was the last message to push, reset to clock
            # if len(messageQueue) == 0:
            if activeQueue:
                client.publish("ledSign/mode", "bigClock")
                mqttSignMode="bigClock"
                activeQueue = False
            await asyncio.sleep(.01) 
            
        else:
            activeQueue=True
            try:
                message = messageQueue.pop(0)
                # for message in messageQueue:
                print("sending message from cache")
                print(message[3])
                logger.info("sending message from cache")
                logger.info(message[3])
                client.publish("ledSign/message/type", message[1])
                client.publish("ledSign/message/sender", message[2])
                client.publish("ledSign/message/text", message[3])
                client.publish("ledSign/mode", "message")
                mqttSignMode="message"
                # await asyncio.sleep(4) 
                msgDisplayTime=6

                if len(message[3])>16:
                    msgDisplayTime += (len(message[3]) - 16)*.75

                # if message[3]
                time.sleep(msgDisplayTime)

            except Exception:
               pass
    






#setup mqtt
mqttBroker = os.environ.get('MQTT_BROKER')
client = mqtt.Client("HASS_messageprocesser")
client.username_pw_set(os.environ.get('MQTT_USER'), password=os.environ.get('MQTT_PASSWORD'))

#connect to mqtt broker function
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    #subcribe to topic to see if the mute status is changed remotely
    



#SIGNAL STUFF
bus = SystemBus()
glibloop = GLib.MainLoop()
glibcontext = glibloop.get_context()
signal = bus.get("org.asamk.Signal", "/org/asamk/Signal/_17814134149")
signal.onMessageReceived = receiveSignalMessage

async def monitorSignalBus():
    # # await glibloop.run()
    # await asyncio.sleep(.1)

    while True:
        await asyncio.sleep(.1)
        #false is needed to keep the thread from getting trapped
        glibcontext.iteration(False)


async def updateTODOs():
    while True:
        await asyncio.sleep(5)
        try:
            #GET WORK TODOs
            headers = {"mode":"getCalendar",
                        "calSource":"IBMLaptop_TODO"}
            result = requests.get(cal_scraper_host, headers=headers)
            todoList=result.text.split("\n")
            #remove last updated timestamp
            todoList = todoList[1:]
            todo = None
            #grab next todo
            if len(todoList) >1:
                todo=todoList[1]
                #trim extra info
                todo = todo.split(',')[0]
                #publish next todo
                client.publish("nextTODO/work", todo)
                
            #GET Personal TODOs
            headers = {"mode":"getCalendar",
                        "calSource":"TO-DO_TODO"}
            result = requests.get(cal_scraper_host, headers=headers)
            todoList=result.text.split("\n")
            #remove last updated timestamp
            todoList = todoList[1:]
            todo = None
            #grab next todo
            if len(todoList) >1:
                todo=todoList[1]
                #trim extra info
                todo = todo.split(',')[0]
                #publish next todo
                client.publish("nextTODO/personal", todo)
        except Exception as e:
            logger.info("US43 exception raised")
            logger.info(e)
            pass



def resetNextEvent():
    nextEvent['eventStart']=None
    client.publish("nextEvent/timeStamp", None)
    nextEvent['eventSubject']=None
    client.publish("nextEvent/subject", None)
    nextEvent['eventOrganizer']=None
    client.publish("nextEvent/organizer", None)
    nextEvent['eventAttendees']=None
    client.publish("nextEvent/attendees", None)
    nextEvent['isExternal']=False
    client.publish("nextEvent/isExternal", False)


    print("NO NEXT EVENT")


#check calendar
#holder for next event
nextEvent = {}
async def updateSchedule():
    while True:
        await asyncio.sleep(5)
        # logger.info("getting new schedule")
        #don't update next event if clock in countdown mode
        if mqttSignMode == "eventCountdown" and nextEvent and "eventStart" in nextEvent:
                nextEventTime=datetime.fromtimestamp(nextEvent['eventStart'])
                secondsUntilNextEvent=(nextEventTime - datetime.now()).seconds
       
                
                #wait to reset sign for 2 minutes
                if datetime.now() < nextEventTime or (datetime.now()-nextEventTime).seconds<120:
                    continue
        try:
            headers = {"mode":"getCalendar",
                        "calSource":"IBMLaptop"}
            result = requests.get(cal_scraper_host, headers=headers)
            schedule=result.text.split("\n")
            #remove last updated timestamp
            schedule = schedule[1:]
            event = None
            #find out next event
            while len(schedule) > 1:
                #get next event from list
                schedule = schedule[1:]
                event = schedule[0].split(",")
                #get time stamp
                eventStart=datetime.utcfromtimestamp(float(event[0]))
                #if the event is in the future, break loop and use that event value
                if eventStart > datetime.now():
                    #this is a tentative event, ignore
                    if "~~" in event[2][:5]:
                        event=None
                        eventStart=None
                    else:
                        break
                #valid event not found, reset event to None 
                else:
                    event=None
                    eventStart=None
                

            
            if eventStart:
                nextEvent['eventStart']=eventStart.timestamp()
                client.publish("nextEvent/timeStamp", nextEvent['eventStart'])
                nextEvent['eventSubject']=event[2].strip()
                client.publish("nextEvent/subject", nextEvent['eventSubject'])
                nextEvent['eventOrganizer']=event[3].strip()
                client.publish("nextEvent/organizer", nextEvent['eventOrganizer'])
                try:
                    if len(event) > 3:
                        nextEvent['eventAttendees']=event[4].strip()
                        if len(nextEvent['eventAttendees'])>200:
                            nextEvent['eventAttendees']=nextEvent['eventAttendees'][0:200]+"....."
                        client.publish("nextEvent/attendees", nextEvent['eventAttendees'])
                    if len(event) > 5 and "EXTATTENDEES" in event[5]:
                        nextEvent['isExternal']=True
                        client.publish("nextEvent/isExternal", True)
                except IndexError:
                    # logger.info("US1354-index error-this is ok")
                    nextEvent['isExternal']=False
                    client.publish("nextEvent/isExternal", False)
                except Exception as e:
                    logger.info("US12")
                    logger.info(e)
                    print(e)
                    nextEvent['isExternal']=False
                    client.publish("nextEvent/isExternal", False)
                print(event)
            else:
                resetNextEvent()
        except Exception as e:
            logger.info("US10 exception raised")
            logger.info(e)
            pass



#check calendar
async def monitorSchedule():
    global mqttSignMode
    while True:
        await asyncio.sleep(.5)

        if nextEvent and "eventStart" in nextEvent and nextEvent['eventStart']:
            try:
                nextEventTime=datetime.fromtimestamp(nextEvent['eventStart'])
                secondsUntilNextEvent=(nextEventTime - datetime.now()).seconds
                if(secondsUntilNextEvent<301):
                    if mqttSignMode != "eventCountdown" or secondsUntilNextEvent % 10 == 0:
                        logger.info("ALERT ABOUT THIS EVENT")
                        client.publish("ledSign/mode", "eventCountdown")
                        if "isExternal" in nextEvent and nextEvent['isExternal']:
                            client.publish("beaconLight/activate", True)
                        else:
                            client.publish("beaconLight/activate", False)
                        mqttSignMode = "eventCountdown"
                elif datetime.now() > nextEventTime and (datetime.now()-nextEventTime).seconds>120:
                # elif datetime.now() > nextEventTime and (datetime.now()-nextEventTime).seconds>120 or secondsUntilNextEvent>600 :
                    #keep from kicking sign out of another mode
                    if mqttSignMode == "eventCountdown":
                        logger.info("clean up alert")
                        client.publish("ledSign/mode", "bigClock")
                        mqttSignMode = "bigClock"
                    resetNextEvent()
                
                #reset beacon light T-3
                if (nextEventTime - datetime.now()).seconds<3 or datetime.now()>nextEventTime:
                    client.publish("beaconLight/activate", False)
                

                

            except Exception as e:
                logger.info("MSERROR 22")
                logger.info(e)
                pass

     
def on_disconnect(client, userdata, rc):
    print("DISCONNECT!!!!! disconnected with rtn code [%d]"% (rc) )
    time.sleep(5)
    print("WAITED 5 seconds...restarting script")
    sys.stdout.flush()
    os.execv(sys.executable, [sys.executable, __file__] + [sys.argv[0]])

    
def on_connect_fail(client, userdata, rc):
    print("CONNECT FAIL with rtn code [%d]"% (rc) )
    print("WAITED 5 seconds...restarting script")
    sys.stdout.flush()
    os.execv(sys.executable, [sys.executable, __file__] + [sys.argv[0]])



async def main(): 
    print("starting mqtt server")
    logger.info("in async")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_connect_fail = on_connect_fail
    client.connect(mqttBroker, 1883)
    client.loop_start()

    #reset sign
    client.publish("ledSign/mode", "bigClock")    

    #start async tasks
    websocketListen = asyncio.create_task(monitorHASSSocket())
    signalListen = asyncio.create_task(monitorSignalBus())
    todoUpdater = asyncio.create_task(updateTODOs()) 
    scheduleUpdater = asyncio.create_task(updateSchedule()) 
    scheduleMonitor = asyncio.create_task(monitorSchedule()) 
    messageSender = asyncio.create_task(initMessageSender()) 
    # try:
        # print("BRIDGE STARTING UP")
        # loop.run()
    # except Exception as e:
        # print (e.message, e.args)


    await websocketListen
    await signalListen
    await todoUpdater
    await messageSender
    await scheduleUpdater
    await scheduleMonitor


if __name__ == "__main__":    
    #START MAIN LOOP
    print("main loop init")
    logger.info("test log-main loop init")
    try:
        asyncio.run(main())   
    except ConnectionRefusedError:
        logger.info("connection refused error")
    except Exception as err:
        logger.info("other error")
        logger.info(err)
    # messageQueue.append(datetime.now(),"IBM","TEST MSG","LONG TEST MESSAGE")