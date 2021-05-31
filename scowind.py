#   Step 1: Fetch data from Cowin site
#   Step 2: Exclude aleardy sent notification
#   Step 3: Filter irrelevant CenterCodes  
#   Step 4: Filter based on age
#   Step 5: Create Telegram message and send
#
# #import all relevant libraries 
import requests
import datetime
import json
import urllib3
import pandas as pd
from simplejson import JSONDecodeError
from datetime import date
import urllib.request
import schedule
import time
import ssl
import random
from datetime import datetime

# Introduce delay
def live():
            tmsgslist = ["The only people who never fail are those who never try.",
                         "Together! We will overcome this!",
                         "All izz well",
                         "We get stronger and more resilient",
                         "Stay safe! Stay indoors",
                         "Everything will be okay in the end. If it's not okay, it's not the end.",
                         "Hang in there, as better times are ahead",
                         "It might be storm now, but storms get tired too",
                         "Every journey begins with a single step!",
                         "It will get better"]
            mlen = len(tmsgslist)
           # print(mlen)
            rrand = random.randint(0, mlen)
            tmsg = tmsgslist[rrand-1]
            #tmsg = 'Hello. Get ready for vaccine slot booking' 
            turl = 'https://api.telegram.org/<id>/sendMessage?chat_id=-<id2>&parse_mode=Markdown&text=' + urllib.parse.quote(tmsg)
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
            result = requests.get(turl, headers=headers)

# The actual script                                   
def thescript():
        print(datetime.now())
        #live()
        #url = 'https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode=455001&date=13-05-2021'
        today = date.today()
        ssl._create_default_https_context = ssl._create_unverified_context
        # dd/mm/YY
        d1 = today.strftime("%d-%m-%Y")
        #Districts to be tracked
        disdict = (
         { "disid": "324",
          "disname": "Dewas"},
         # { "disid": "314",
         # "disname": "Indore"},    
         { "disid": "363",
          "disname": "Pune"},
         { "disid": "395",
          "disname": "Mumbai"},
         { "disid": "392",
          "disname": "Thane"} 
        )
        for z in disdict:
            cf = 'cache_'+ z["disid"] + '.txt'
            f = open(cf, "r")
            dfc = pd.read_csv(cf, index_col=0)
            f.close()
            url = 'https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=' + z["disid"] +'&date='+d1
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
            result = requests.get(url, headers=headers)
            data = json.loads(result.content.decode())
            #ddata = json.loads(result.content.decode())
            data = (data['centers'])
            #print(result.content.decode())
            df = pd.DataFrame.from_dict(data)
            dff = pd.DataFrame(columns=('a', 'b', 'c','d','e','f','g', 'h','i','j','k','l','m'))
            i = 1
            df = pd.DataFrame(columns=('a', 'b', 'c','d','e','f','g', 'h','i','j','k','l','m'))
            #Get CenterCodes to be excluded
            response = requests.get('https://raw.githubusercontent.com/deolekar/intro-to-python/master/cowinconfig.txt')
            assert response.status_code == 200, 'Wrong status code'
            dresp = response.content.decode()
            dresp = dresp.split(',')
            dresp = [int(i) for i in dresp]
            #print(dresp)
            for x in data:
                #print("==" "--")
                for y in x['sessions']:
                    if (y['available_capacity']> 0):
                        #print(x['center_id'],",", x['name'], ",", x['pincode'],",", x['fee_type'], ",",y['date'],",",y['available_capacity'],",",y['min_age_limit'], y['vaccine'])
                        df.loc[i] = [x['center_id'],x['name'],x['pincode'],x['fee_type'],y['date'],y['available_capacity'],y['min_age_limit'],y['vaccine'], str(x['center_id'])+x['name']+str(x['pincode'])+x['fee_type']+y['date']+str(y['available_capacity'])+str(y['min_age_limit'])+y['vaccine'],x['address'],y['available_capacity_dose1'],y['available_capacity_dose2'],x['block_name']]
                        i = i + 1
                        #pd.DataFrame([flatten_json(x) for x in ddata['centers']])

            #Exclude records for certain CenterCodes configured in txt on github
            df = df[~df["a"].isin(dresp)]
            dff = dff.append(df)
            #df = df[~df.isin(df2)].dropna()
            #df = df[~(df['a'].isin(dfc['a']) & df['f'].isin(dfc['f']) & df['c'].isin(dfc['c']) & df['d'].isin(dfc['d']) & df['e'].isin(dfc['e']) & df['f'].isin(dfc['f']) & df['g'].isin(dfc['g']) & df['h'].isin(dfc['h']))]
            #Exclude already sent notification
            df = df[~(df['i'].isin(dfc['i']) )]
            
         
            count_row = df.shape[0]
            flag = 'y'
            if (count_row >0 ):
               #Exclude some notifications 
               for t in range(count_row):
                    if (z["disname"] == 'Indore' and int(df.iloc[t]['g']) < 40 ):
                        flag = 'n'
                    if (z["disname"] == 'Dewas' and int(df.iloc[t]['g']) < 40 ):
                        flag = 'n'                   
                    if (z["disname"] == 'Mumbai' and int(df.iloc[t]['g']) > 40 ):
                        flag = 'n'
                    if (z["disname"] == 'Thane' and int(df.iloc[t]['g']) > 40 ):
                        flag = 'n'
                    if (z["disname"] == 'Pune' and int(df.iloc[t]['g']) > 40 ):
                        flag = 'n'                        

                    #Prepare T message
                    if (df.iloc[t]['h'] != 'COVAXIN' and flag == 'y'):
                        tmsg = ("District:"+  z["disname"]+ ",\n Center Id:"+ str(df.iloc[t]['a']) + ",\n Center:" + str(df.iloc[t]['b']) + ",\n PinCode:" + str(df.iloc[t]['c']) + ",\n Fee Type:" + str(df.iloc[t]['d'])  + ",\n Date:" + str(df.iloc[t]['e']) + ",\n Available Capacity:" + str(df.iloc[t]['f']) + ",\n Dose1:" + str(df.iloc[t]['k']) + ",\n Dose2:" + str(df.iloc[t]['l'])+ ",\n Min Age:" + str(df.iloc[t]['g'])  + ",\n Vaccine:" + str(df.iloc[t]['h'] ) + ", \n\n Address:" + str(df.iloc[t]['j']) + ",\n Block:"+ str(df.iloc[t]['m']) )
                        #tmsg = ("District:"+  z["disname"]+ ", Center Id:"+ str(df.iloc[t]['a']) + ", Center:" + str(df.iloc[t]['b']) + ", PinCode:" + str(df.iloc[t]['c']) + ", Fee Type:" + str(df.iloc[t]['d'])  + ", Date:" + str(df.iloc[t]['e']) + ", Available Capacity:" + str(df.iloc[t]['f']) + ", Dose1:" + str(df.iloc[t]['k']) + ", Dose2:" + str(df.iloc[t]['l'])+ ", Min Age:" + str(df.iloc[t]['g'])  + ", Vaccine:" + str(df.iloc[t]['h'] ) + ", \n\n Address:" + str(df.iloc[t]['j']) + ",\n Block:"+ str(df.iloc[t]['m']) )
                        print (tmsg)
                        #tmsg = x['center_id'],",", x['name'], ",", x['pincode'],",", x['fee_type'], ",",y['date'],",",y['available_capacity'],",",y['min_age_limit'], y['vaccine'
                        turl = 'https://api.telegram.org/<id1>/sendMessage?chat_id=-<id2>&parse_mode=Markdown&text=' + urllib.parse.quote(tmsg)
                        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
                        result = requests.get(turl, headers=headers)
                        #urllib.request.urlopen(turl)
         
                    with open(cf,'a',encoding = 'utf-8') as f:
                        dff.to_csv(cf)
                        f.close()
                        #print(df)

schedule.every(60).seconds.do(thescript)
schedule.every().day.at("01:30").do(live)
schedule.every().day.at("05:30").do(live) 
schedule.every().day.at("09:30").do(live)
schedule.every().day.at("13:30").do(live)
schedule.every().day.at("16:30").do(live)

while 1:
    schedule.run_pending()
    time.sleep(1)
