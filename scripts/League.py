
# coding: utf-8

# In[193]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
import re
import urllib.request
import bs4
import ast
import math
import datetime as dt
import time

import requests
import lxml.html as lh
import pandas as pd

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import date
import os


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

from scipy import stats
from scipy.stats import norm


def upload_drive(rating,sheets,str_):
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

    today = date.today()
    m = today.strftime("%d%m%Y")
    dir_ = 'save/' + m
    try:
        os.mkdir(dir_)
    except:
        pp = 0
    

    with pd.ExcelWriter(dir_ + '/' + str_) as writer:  
        for k in range(0,len(rating)):
            rating[k].to_excel(writer,sheet_name = sheets[k],index = False,float_format="%.4f")

    test_file = drive.CreateFile({'title': str_})
    test_file.SetContentFile(dir_ + '/' + str_)
    test_file.Upload({'convert': True})
    

def ab(a,b):
    
    if b != b:
        return a
    else:
        return((a+b)/2)



def ban_phase(url):
    dict_team = {'G2 Esports':'G2','Rogue (European Team)':'RGE','Fnatic':'FNC','MAD Lions':'MAD','SK Gaming' : 'SK',
            'FC Schalke 04 Esports' : 'S04','Misfits Gaming' :'MSF','Team Vitality':'VIT','Origen':'OG','Excel Esports':'XL'}

    #Create a handle, page, to handle the contents of the website
    page = requests.get(url)
    #Store the contents of the website under doc
    doc = lh.fromstring(page.content)
    #Parse data that are stored between <tr>..</tr> of HTML
    tr_elements = doc.xpath('//tr')

    tr_elements = doc.xpath('//tr')
    #Create empty list
    col=[]
    i=0
    #For each row, store each first element (header) and an empty list
    for t in tr_elements[1]:
        
        i+=1
        name=t.text_content()
        

        col.append((name,[]))

    for j in range(2,len(tr_elements)):
        #T is our j'th row
        T=tr_elements[j]

        #If row is not of size 10, the //tr data is not from our table 
        if len(T)!=32:
            break

        #i is the index of our column
        i=0

        #Iterate through each element of the row
        for t in T.iterchildren():
            data=t.text_content() 
            #Check if row is empty
            print(data)


            #Append the data to the empty list of the i'th column
            col[i][1].append(data)
            #Increment i for the next column
            i+=1
            
    Dict={title:column for (title,column) in col}
    df=pd.DataFrame(Dict)
    df['Blue'] = df['Blue'].replace(dict_team)
    df['Red'] = df['Red'].replace(dict_team)
    
    return(df)

def outs(tops_output,tops,week):
    tops_2020 = tops_output[tops_output['player'].apply(lambda x : check_2020(x)) == True].sort_values('player')
    tops_2020['player'] = tops_2020['player'].apply(lambda x : x.replace(' 2020',''))
    tops_2020 = tops_2020.rename(columns = {'rating':'Mid-Rating_2020'})
    tops_2019 = tops_output[tops_output['player'].apply(lambda x : check_2020(x)) == False].sort_values('player')
    tops_zer = tops_2020.merge(tops_2019,left_on = 'player',right_on = 'player',how = 'outer' )
    
    tops_zer = tops_zer[tops_zer['player'].isin(tops)]
    tops_zer = tops_zer.rename(columns = {'rating' : 'Mid-Rating'})
    
    tops_zer['FinalRatingWeek' + str(week)] = tops_zer[['Mid-Rating_2020','Mid-Rating']].apply(lambda x : ab(*x),axis = 1)
    
    return(tops_zer)

def check_2020(x):
    if '2020' in x:
        return True
    return False


def ic(rating):
    m = rating.mean()
    std = rating.std()
    n = len(rating)
    ic = [m - ((10/100)*std/(n**(1/2))),(m + (90/100)*std/(n**(1/2)))]
    return(ic)

def ratings(df,L,shift):
    out = []
    
    for j in df['player']:
        val = 0
        for i in L:
            val+= df[df['player'] == j][i[0]].values[0]*i[1]*100
            
        val = val/2 + shift
        out.append([j,val])
        
    return(pd.DataFrame(out,columns = ['player','rating']))
        
def stats_ratings(df_l1_,df1_,pos_,stats_,coef_,tops_,shift1 = 50,shift2 = 50,players_to_add = [],to_invert = None,to_invert_2 = None):
    
    
    top_lec = get_ratings(df_l1_,pos_,stats_,coef_,shift1,to_invert,to_invert_2)
    titu_erl = find_titulaire(df1_)
    df_erl = titu_erl[titu_erl['titulaire'] == True]
    df_erl = df_erl.append(titu_erl[titu_erl['player'].isin(players_to_add)])   
    
    top_erl = get_ratings(df_erl,pos_,stats_,coef_,shift2,to_invert,to_invert_2)
    
    og = [i for i in top_lec['player'].tolist() if i in tops_]
    
    ng = [k for k in tops_ if k not in og]

    
    top_erl = top_erl[top_erl['player'].isin(ng)]
    top_lec = top_lec[top_lec['player'].isin(og)]
    
    toppy = top_erl.append(top_lec)
    toppy = toppy[toppy['player'].isin(tops_)]
    
    df_l1['LEC'] = True
    df_erl['LEC'] = False

    
    df_lec = df_l1[['player'] + stats_ + ['gp','LEC','pos1champs']][df_l1['player'].isin(og)]
    df_erl = df_erl[['player'] + stats_ + ['gp','LEC','pos1champs']][df_erl['player'].isin(ng)]
    
        

    df_fin = df_lec.append(df_erl)
    
    df_fin = df_fin.merge(toppy,left_on ='player',right_on ='player')
    
    df_zer = df_fin[['player','rating','gp','LEC','pos1champs'] + stats_ ]
    
    
    
    return(df_zer.rename(columns = {'timeCCingOthers' : 'CCtime','pos1uniquescore' :'UniqueWinChamps'}))

def stats_ratings_2(df_l1_,pos_,stats_,coef_,tops_,shift1 = 50,to_invert = None,to_invert_2 = None):
    
    top_lec = get_ratings(df_l1_,pos_,stats_,coef_,shift1,to_invert,to_invert_2)
    
    
    return(top_lec)

def outputaxelle(df,df2,stats_to_show,stat,players,week,to_invert = None,to_invert_2 = None,outs_ = True):
    
    stats_to_show_2 = stats_to_show
    
    dict_ = {}
    dict_inv = {}
    if to_invert != None:
    
        for i in range(0,len(to_invert)):
            if to_invert_2[i] == True:                
                df[to_invert[i] + '2'] = 100-df[to_invert[i]]
            else:
                df[to_invert[i] + '2'] = 1/(1+df[to_invert[i]])
        stats_to_show_2 = []
        
        
        for j in stats_to_show:
            if j in to_invert:
                stats_to_show_2.append(j + '2')
                dict_[j] = j + '2'
                dict_inv[j + '2'] = j
            else:
                stats_to_show_2.append(j)
    
    
    stats_out1 = []
    stats_out2 = []
    for i in stats_to_show:
        stats_out1.append(i + 'r')
        stats_out2.append(i + 'p')
        dict_inv[i + 'r'] = (i+'r').replace('2','')
        dict_inv[i + 'p'] = (i+'p').replace('2','')        
        

    for j in range(0,len(stats_out1)):
        dict_[stats_to_show_2[j]] = stats_out1[j]
        
        

        
    df_ranks = df[['player'] + stats_to_show_2].set_index('player').rank(ascending = False,method = 'min').reset_index().sort_values('player')
    df_ranks = df_ranks.rename(columns = dict_)
    
    for j in range(0,len(stats_out2)):
        df[stats_out2[j]] = np.round(norm.cdf(stats.zscore(df[[stats_to_show[j]]])),2)
        
        
        
    df_out = df.merge(df_ranks,left_on = 'player',right_on = 'player')
    df_out = df_out[['player'] + stat + stats_out1 + stats_out2]
    
    M = ['player']
    for j in stat:
        if j in stats_to_show:
            M.append(j)
            M.append(j + 'r')
            M.append(j + 'p')
        else:
            M.append(j)
    
    if outs_ == True:
        outie = outs(df2,players,week)
    else:

        outie = df2


    outie  = outie.merge(df_out.rename(columns = dict_inv)[M].sort_values('player'),left_on = 'player',right_on = 'player')

    outie['rating'] = outie['rating'].apply(lambda x : np.round(x,1))
    return(outie)
    
        
    
    

def get_ratings(df,pos,stats,coef,shift = 50,to_invert = None,to_invert_2 = None,df_stat = None):
    
    
    if df_stat != None:
        rat = df[df['main_position'] == pos][['player'] + stats].set_index('player').rank(pct = True).reset_index()
        return(rat)
    else:
        rat = df[df['main_position'] == pos][['player'] + stats].set_index('player').rank(pct = True).reset_index()

    if to_invert == None:
        # print(ratings(rat,coef,shift).std())
        return(ratings(rat,coef,shift).sort_values('rating',ascending = False))
    
    else:
        
        for i in range(0,len(to_invert)):
            if to_invert_2[i] == True:                
                rat[to_invert[i]] = 1-rat[to_invert[i]]
            else:
                rat[to_invert[i]] = 1/(1+rat[to_invert[i]])
        # print(ratings(rat,coef,shift).std())
        return(ratings(rat,coef,shift).sort_values('rating',ascending = False))

def read_mh(df_z):
    
    df_mh = pd.read_csv(df_z)
    df_mh['game_json'] = df_mh['game_json'].apply(ast.literal_eval)
    df_mh['timeline_json'] = df_mh['timeline_json'].apply(ast.literal_eval)
    return(df_mh)


def find_titulaire(df):
    unique_teams = df['team'].unique().tolist()
    pos = ['Top','Jungle','Mid','ADC','Support']
    
    df['titulaire'] = False
    
    teams=[]
    
    
    
    for k in unique_teams:
        df_work = df[df['team'] == k]
        
        for j in pos:            
#             print(k,j)
            player = df[df['team'] == k][df['main_position'] == j].sort_values('gp',ascending=False)['player'].values[0]
            
            df_work.loc[df_work['player'] == player,'titulaire'] =  True
        teams.append(df_work)
        
        
        
    return(pd.concat(teams))
        
    
    
    

# ## First Functions

# In[2]:


def killsassists15(js,jg,champ,j):
    ww = jg['frames'][0:15]
    m = 0
    for i in ww:
        for k in i['events']:
            if k['type'] == 'CHAMPION_KILL':
                tab = [k['killerId']] + k['assistingParticipantIds']
                if (j+1) in tab:
                    m+=1
    return(m)


def deaths15(js,jg,champ,j):
    ww = jg['frames'][0:15]
    m = 0
    for i in ww:
        for k in i['events']:
            if k['type'] == 'CHAMPION_KILL':
                if j+1 == k['victimId']:
                    m+=1
    return(m)

def soloKills(js,jg,champ,j):
    count = 0
    id_zer = j+1
    for i in jg['frames']:
        for k in i['events']:
            if k['type'] == 'CHAMPION_KILL':
                dez = k
                if dez['killerId'] == id_zer:
                    if len(dez['assistingParticipantIds']) == 0:
                        count+=1
    return(count)

def katz(x):
    millis=x
    millis = int(millis)
    seconds=(millis/1000)%60
    seconds = int(seconds)
    minutes=(millis/(1000*60))%60
    minutes = int(minutes)
    if seconds <= 9:
        return("%d:%d%d" % (minutes,0,seconds))
    return ("%d:%d" % (minutes, seconds))

def fb_ij(jg_):
    jg = jg_['frames']
    p,k = 0,0
    i = 0
    fb_b = False
    while i < len(jg) and fb_b == False:
        m = len(jg[i]['events'])
        j = 0
        while j < m and fb_b == False:
            if jg[i]['events'][j]['type'] == 'CHAMPION_KILL':
                fb_b = True
                p,k = i,j

                return(jg[p]['events'][k])
            j+=1
        i+=1

    return(False)


def fbVictim(js,jg,champ,j):

    ev = fb_ij(jg)
    if ev['victimId'] == j+1:
        return(True)
    else:
        return(False)




def json_game(mh):
    root = 'https://acs.leagueoflegends.com/v1/stats/game'
    rac = mh.split('#match-details')[1].split('&tab=overview')[0]
    url = root + rac
    return(url)

def seconds_to_min(k):

    minz = k//60
    secz = k%60

    return(str(int(minz)) + ':' + str(int(secz)))

def json_timeline(mh):
    root = 'https://acs.leagueoflegends.com/v1/stats/game'
    rac = mh.split('#match-details')[1].split('&tab=overview')[0]
    url = root + rac
    url = url.replace('?','/timeline?')
    return(url)


def get_request(urlz,cookie):

    d = requests.get(url = urlz,cookies = cookie)
    if d.status_code != 200:
        return(d.status_code)
    else:
        return(d.json())



columns = ['gameid','patch','team','side','gameduration','win','firstBlood', 'firstTower', 'firstInhibitor', 'firstBaron',
           'firstDragon', 'firstRiftHerald','towerKills', 'inhibitorKills', 'baronKills', 'dragonKills',
           'riftHeraldKills','top','jgl','mid','adc','sup',
           'ban1','ban2','ban3','ban4','ban5','top_pick','jgl_pick','mid_pick','adc_pick','sup_pick']


# ## Team Functions

# In[3]:


def get_champs(js,champ):
    blue = js['participants'][0:5]
    red = js['participants'][5:]
    blues = []
    reds = []
    for i in blue:
        blues.append(champ[champ['key'] == i['championId']]['name'].values[0])

    for i in red:
        reds.append(champ[champ['key'] == i['championId']]['name'].values[0])

    return([blues,reds])



def name_(x):
    p = x.split(' ')
    try:
        name = p[1]
    except:
        name = p[0]
    i = 2
    while i < len(p):

        name += ' ' + p[i]
        i+=1

    return name

def get_name(js):
    blue = js['participantIdentities'][0:5]
    red = js['participantIdentities'][5:]


    blues = []
    reds = []
    for i in blue:
        blues.append(name_(i['player']['summonerName']))

    for j in red:
        reds.append(name_(j['player']['summonerName']))

    return([blues,reds])


def bans(js,champ):

    blue_bans = js['teams'][0]['bans']
    red_bans = js['teams'][1]['bans']

    ban_blue = []
    ban_red = []
    for i in blue_bans:
#         print(i)
        ban_blue.append(champ[champ['key'] == i['championId']]['name'].values[0])

    for i in red_bans:
        ban_red.append(champ[champ['key'] == i['championId']]['name'].values[0])


    r = len(ban_red)
    b = len(ban_blue)

    if r != 5:
        for i in range(r,5):
            ban_red.append('No Ban')

    if b != 5:
        for i in range(b,5):
            ban_blue.append('No Ban')


    return([ban_blue,ban_red])

def stats_team(js,champ,jg):
    M = []
    output_1 = []
    output_2 = []

    output_1.append(js['gameId'])
    output_2.append(js['gameId'])
    output_2.append(js['gameVersion'][0:4])
    output_1.append(js['gameVersion'][0:4])


    output_1.append(js['participantIdentities'][0]['player']['summonerName'].split(' ')[0])
    output_2.append(js['participantIdentities'][5]['player']['summonerName'].split(' ')[0])

    output_1.append('Blue')
    output_2.append('Red')

    blue = js['teams'][0]
    red = js['teams'][1]
    output_1.append(seconds_to_min(js['gameDuration']))
    output_2.append(seconds_to_min(js['gameDuration']))

    m = ['win', 'firstBlood', 'firstTower', 'firstInhibitor', 'firstBaron', 'firstDragon', 'firstRiftHerald',
         'towerKills', 'inhibitorKills', 'baronKills', 'dragonKills','riftHeraldKills']

    for i in m:
        if i == 'win':

            output_1.append(blue[i].replace('Win','1').replace('Fail','0'))
            output_2.append(red[i].replace('Win','1').replace('Fail','0'))
        else:
            output_1.append(blue[i])

            
            output_2.append(red[i])




    ##players_name

    names = get_name(js)

    for p in names[0]:
        output_1.append(p)

    for p in names[1]:
        output_2.append(p)


    ##bans



    L = bans(js,champ)

    for i in L[0]:
        output_1.append(i)

    for i in L[1]:
        output_2.append(i)

    ### picks

    zzz = get_champs(js,champ)

    for i in zzz[0]:
        output_1.append(i)

    for i in zzz[1]:
        output_2.append(i)

#Golds + kills


    summ_blue = 0
    summ_red = 0
    kills_blue = 0
    kills_red = 0
    deaths_blue = 0
    deaths_red = 0
    assists_red = 0
    assists_blue = 0
    for j in range(0,5):
        
        summ_blue += js['participants'][j + 0]['stats']['goldEarned']
        summ_red += js['participants'][j + 5]['stats']['goldEarned']


        kills_blue += js['participants'][j + 0]['stats']['kills']
        kills_red += js['participants'][j + 5]['stats']['kills']
        deaths_blue += js['participants'][j + 0]['stats']['deaths']
        deaths_red += js['participants'][j + 5]['stats']['deaths']
        assists_blue += js['participants'][j + 0]['stats']['assists']
        assists_red += js['participants'][j + 5]['stats']['assists']


    output_1.append(summ_blue)
    output_2.append(summ_red)

    output_1.append(summ_blue/gametime(output_1[4]))
    output_2.append(summ_red/gametime(output_1[4]))

    output_1.append(summ_red)
    output_2.append(summ_blue)

    #GD@15 et #GD@20

    totalgd15blue = 0
    totalgd15red = 0

    totalgd20blue = 0
    totalgd20red = 0

    for j in range(1,6):
        totalgd20blue += jg['frames'][20]['participantFrames'][str(j)]['totalGold']
        totalgd20red += jg['frames'][20]['participantFrames'][str(j+5)]['totalGold']

        totalgd15blue += jg['frames'][15]['participantFrames'][str(j)]['totalGold']
        totalgd15red += jg['frames'][15]['participantFrames'][str(j+5)]['totalGold']

    output_1.append(totalgd20blue)
    output_1.append(totalgd20red)
    output_1.append(totalgd20blue - totalgd20red)

    output_1.append(totalgd15blue)
    output_1.append(totalgd15red)
    output_1.append(totalgd15blue - totalgd15red)
    
    output_2.append(totalgd20red)
    output_2.append(totalgd20blue)
    output_2.append(totalgd20red - totalgd20blue)

    output_2.append(totalgd15red)
    output_2.append(totalgd15blue)
    output_2.append(totalgd15red - totalgd15blue)





    output_1.append(kills_blue)
    output_1.append(deaths_blue)
    output_1.append(assists_blue)


    output_2.append(kills_red)
    output_2.append(deaths_red)
    output_2.append(assists_red)

    ##Towers

    towersKills_red = 0
    towersKills_blue = 0

    for j in range(0,len(jg['frames'])):
        p = jg['frames'][j]          
        for k in p['events']:
            if k['type'] == 'BUILDING_KILL':
                if k['teamId'] == 100:
                    towersKills_red +=1
                else:
                    towersKills_blue += 1


    towersKills_red15 = 0
    towersKills_blue15 = 0

    for j in range(0,16):
        p = jg['frames'][j]          
        for k in p['events']:
            if k['type'] == 'BUILDING_KILL':
                if k['teamId'] == 100:
                    towersKills_red15 +=1
                else:
                    towersKills_blue15 += 1


    towersKills_red10 = 0
    towersKills_blue10 = 0

    for j in range(0,11):
        p = jg['frames'][j]          
        for k in p['events']:
            if k['type'] == 'BUILDING_KILL':
                if k['teamId'] == 100:
                    towersKills_red10 +=1
                else:
                    towersKills_blue10 += 1

    towersKills_red20 = 0
    towersKills_blue20 = 0



    for j in range(0,21):
        p = jg['frames'][j]          
        for k in p['events']:
            if k['type'] == 'BUILDING_KILL':
                if k['teamId'] == 100:
                    towersKills_red20 +=1
                else:
                    towersKills_blue20 += 1

    dragonsred15 = 0
    dragonsblue15 = 0

    dragonsred20 = 0
    dragonsblue20 = 0

    for j in range(0,len(jg['frames'])):
        p = jg['frames'][j]          
        for k in p['events']:
            
            
            if k['type'] == 'ELITE_MONSTER_KILL' and k['monsterType'] == 'DRAGON':
                if k['killerId'] >5:
                    if j<= 20:
                        dragonsred20+=1
                        if j<=15:
                            dragonsred15+=1
                        
                else:
                    if j<= 20:
                        dragonsblue20+=1
                        if j<=15:
                            dragonsblue15+=1
                

       
    frames = [5,10,15,20]
    dict__blue = {4 : 0,9 : 0, 14 : 0, 19 : 0}
    dict__red = {4 : 0,9 : 0, 14 : 0, 19 : 0}

    for i in frames:

        for j in range(0,i):
            p = jg['frames'][j]          
            for k in p['events']:
                if k['type'] == 'CHAMPION_KILL':
                    
                    if k['killerId'] >= 6:
                        dict__red[i-1]+= 1
                    else:
                        dict__blue[i-1]+=1




    output_1.append(towersKills_red)
    output_1.append(towersKills_blue10)
    output_1.append(towersKills_red10)
    output_1.append(towersKills_blue15)
    output_1.append(towersKills_red15)
    output_1.append(towersKills_blue20)
    output_1.append(towersKills_red20)


    output_2.append(towersKills_blue)
    output_2.append(towersKills_red10)
    output_2.append(towersKills_blue10)
    output_2.append(towersKills_red15)
    output_2.append(towersKills_blue15)
    output_2.append(towersKills_red20)
    output_2.append(towersKills_blue20)

    output_1.append(-(towersKills_red10 - towersKills_blue10))
    output_2.append((towersKills_red10 - towersKills_blue10))


    output_1.append(-(towersKills_red15 - towersKills_blue15))
    output_1.append(-(towersKills_red20 - towersKills_blue20))

    output_2.append((towersKills_red15 - towersKills_blue15))
    output_2.append((towersKills_red20 - towersKills_blue20))


    output_1.append(dragonsblue15)
    output_1.append(dragonsblue20)

    output_2.append(dragonsred15)
    output_2.append(dragonsred20)
    
    output_1.append(100*output_1[15]/(output_1[15] + output_2[15]))
    output_2.append(100*output_2[15]/(output_1[15] + output_2[15]))


    
    heraldsred15 = 0
    heraldsblue15 = 0

    heraldsred20 = 0
    heraldsblue20 = 0

    for j in range(0,len(jg['frames'])):
        p = jg['frames'][j]          
        for k in p['events']:
            
            
            if k['type'] == 'ELITE_MONSTER_KILL' and k['monsterType'] == 'RIFTHERALD':
                if k['killerId'] >5:
                    if j<= 20:
                        heraldsred20+=1
                        if j<=10:
                            
                            heraldsred15+=1
                        
                else:
                    if j<= 20:
                        heraldsblue20+=1
                        if j<=10:
                            heraldsblue15+=1

    output_1.append(heraldsblue15)
    output_1.append(heraldsblue20)

    output_2.append(heraldsred15)
    output_2.append(heraldsred20)

    output_1.append(100*heraldsblue20/(2))
    output_2.append(100*heraldsred20/(2))



    for j in dict__red:
        output_1.append(dict__blue[j])
        output_2.append(dict__red[j])
        
        output_1.append(dict__red[j])
        output_2.append(dict__blue[j])

    baron_blue = 0
    baron_red = 0
    for j in range(0,len(jg['frames'])):
        p = jg['frames'][j]          
        for k in p['events']:


            if k['type'] == 'ELITE_MONSTER_KILL' and k['monsterType'] == 'BARON_NASHOR':
                if k['killerId'] >5:
                    baron_red+=1
                else:
                    baron_blue+=1


    # output_1.append(baron_blue)
    # output_2.append(baron_red)

    if baron_blue + baron_red != 0:
        output_1.append(100*baron_blue/(baron_blue + baron_red))
        output_2.append(100*baron_red/(baron_blue+baron_red))

    else:
        output_1.append(0)
        output_2.append(0)


    totalcsblue = 0
    totalcsred = 0

    totaldmgblue = 0
    totaldmgred = 0
    totalvsblue = 0
    totalvsred = 0
    totalwpblue = 0
    totalwpred = 0
    totalwpcblue = 0
    totalwpcred = 0
    totalccTimeblue = 0
    totalccTimered = 0


    for j in range(0,10):
        if j<=4:
            totalcsblue+= js['participants'][j]['stats']['totalMinionsKilled'] + js['participants'][j]['stats']['neutralMinionsKilled']
            totaldmgblue+= js['participants'][j]['stats']['totalDamageDealt']
            totalvsblue+= js['participants'][j]['stats']['visionScore']
            totalwpblue+= js['participants'][j]['stats']['wardsPlaced']
            totalwpcblue+= js['participants'][j]['stats']['wardsKilled']
            totalccTimeblue += js['participants'][j]['stats']['timeCCingOthers']




        else:
            totalcsred+= js['participants'][j]['stats']['totalMinionsKilled'] + js['participants'][j]['stats']['neutralMinionsKilled']
            totaldmgred+= js['participants'][j]['stats']['totalDamageDealt']
            totalvsred+= js['participants'][j]['stats']['visionScore']
            totalwpred+= js['participants'][j]['stats']['wardsPlaced']
            totalwpcred+= js['participants'][j]['stats']['wardsKilled']
            totalccTimered += js['participants'][j]['stats']['timeCCingOthers']




    
    # print(output_1[0:6])
    output_1.append(totalcsblue/gametime(output_1[4]))
    output_2.append(totalcsred/gametime(output_1[4]))

    output_1.append(totaldmgblue/gametime(output_1[4]))
    output_2.append(totaldmgred/gametime(output_1[4]))

    output_1.append(totalvsblue/gametime(output_1[4]))
    output_2.append(totalvsred/gametime(output_1[4]))


    output_1.append(totalwpblue)
    output_1.append(totalwpcblue)

    output_2.append(totalwpred)
    output_2.append(totalwpcred)

    output_1.append(totalwpblue/gametime(output_1[4]))
    output_1.append(totalwpcblue/gametime(output_1[4]))

    output_2.append(totalwpred/gametime(output_1[4]))
    output_2.append(totalwpcred/gametime(output_1[4]))

    output_1.append(gametime(output_1[4]))
    output_2.append(gametime(output_1[4]))

    visionWPred = 0
    visionWPblue = 0
    for j in jg['frames']:
        for k in j['events']:
            if k['type'] == 'WARD_PLACED':
                if k['wardType'] == 'CONTROL_WARD':
                    if k['creatorId'] >= 5:
                        visionWPred += 1
                    else:
                        visionWPblue += 1


    output_1.append(visionWPblue)
    output_2.append(visionWPred)

    visionWPred = 0
    visionWPblue = 0
    for j in jg['frames']:
        for k in j['events']:
            if k['type'] == 'WARD_KILL':
                if k['wardType'] == 'CONTROL_WARD':
                    if k['killerId'] >= 5:
                        visionWPred += 1
                    else:
                        visionWPblue += 1

    output_1.append(visionWPblue)
    output_2.append(visionWPred)

    output_1.append(totalccTimeblue)
    output_1.append(totalccTimeblue/gametime(output_1[4]))

    output_2.append(totalccTimered)
    output_2.append(totalccTimered/gametime(output_1[4]))

    kills_ = firstKilltime(jg)
    turrets_ = firstTurretTime(jg)
    try:
        output_1.append(kills_[0][0])

    except:
        output_1.append(None)

    try:
        output_1.append(kills_[1][0])

    except:
        output_1.append(None)

    try:

        output_2.append(kills_[1][0])
    except:
        output_2.append(None)

    try:
        output_2.append(kills_[0][0])
    except:
        output_2.append(None)

    try:
        
        output_1.append(turrets_[0][0])
    except:
        output_1.append(None)
    try:
        output_1.append(turrets_[1][0])
    except: 
        output_1.append(None)
    try:
        output_2.append(turrets_[1][0])
    except:
        output_2.append(None)
    try:
        output_2.append(turrets_[0][0])
    except:
        output_2.append(None)

    output_1.append(kills_blue/gametime(output_1[4]))
    output_2.append(kills_red/gametime(output_1[4]))

    

    return([output_1,output_2])


def concat_3(x,y,z):
    return([x,y,z])

def team_statistics(df,champ):

    columns = ['league','gameid','patch','team','side','gameduration','win','firstBlood', 'firstTower', 'firstInhibitor', 'firstBaron',
           'firstDragon', 'firstRiftHerald','towerKills', 'inhibitorKills', 'baronKills', 'dragonKills',
           'riftHeraldKills','top','jgl','mid','adc','sup',
           'ban1','ban2','ban3','ban4','ban5','top_pick','jgl_pick','mid_pick','adc_pick','sup_pick','totalGold','GPM','totalOppositeGold','totalGold@20','totalGoldOpp@20','GD@20',
           'totalGold@15','totalGoldOpp@15','GD@15',
           
           'totalKills','totalDeaths',
           'totalAssists','towersLost','towersKilled@10','towersLost@10','towersKilled@15','towersLost@15','towersKilled@20','towersLost@20','towersD@10','towersD@15',
           'towersD@20','dragons@15','dragons@20','dragons%','heralds@10','heralds@20','heralds%','kills@5','deaths@5',
           'kills@10','deaths@10','kills@15','deaths@15','kills@20','deaths@20','baron%','cspm','dpm','vspm','totalWardsPlaced','totalWardsCleared',
           'wardsPlacedpm','wardsKilledpm','minutesPlayed','totalVisionWardPlaced','totalVisionWardKilled','totalTimeCrowdControlDealt','totalTimeCrowdControlDealtpm',
           'firstKillTime','firstDeathTime',
           'firstTurretTime','firstTurretLossTime','KillsPerMinute']

    L = []

    df2 =df.copy()
    df2['concat'] = df[['league','game_json','timeline_json']].apply(lambda x : concat_3(*x),axis = 1)

    for i in df2['concat']:

        L.append([i[0]] + stats_team(i[1],champ,i[2])[0])
        L.append([i[0]] + stats_team(i[1],champ,i[2])[1])
    

    return(pd.DataFrame(L,columns = columns))


# ## Players Functions

# In[4]:


def player_j(js,jg,champ,j): #j between 0 and 9



    dic = {0:[0,0,'Top','Blue'],1:[0,1,'Jungle','Blue'],2:[0,2,'Mid','Blue'],3:[0,3,'ADC','Blue'],4:[0,4,'Support','Blue'],
            5:[1,0,'Top','Red'],6:[1,1,'Jungle','Red'],7:[1,2,'Mid','Red'],8:[1,3,'ADC','Red'],9:[1,4,'Support','Red']}

    M = []
    M += [js['gameId'],js['gameVersion'][0:4]]
    M += [js['participantIdentities'][j]['player']['summonerName'].split(' ')[0].upper()]
#     print(len(M))

    M += [get_name(js)[dic[j][0]][dic[j][1]]]
    M += [dic[j][2],dic[j][3],seconds_to_min(js['gameDuration'])]
    M += [get_champs(js,champ)[dic[j][0]][dic[j][1]]]
    an = js['participants'][j]['stats']
    temp_d = ['item0', 'item1', 'item2', 'item3', 'item4', 'item5', 'item6', 'kills', 'deaths', 'assists', 'largestKillingSpree', 
    'largestMultiKill', 'killingSprees', 'longestTimeSpentLiving', 'doubleKills', 'tripleKills', 'quadraKills',
     'pentaKills', 'unrealKills', 'totalDamageDealt', 'magicDamageDealt', 'physicalDamageDealt', 'trueDamageDealt', 
     'largestCriticalStrike', 'totalDamageDealtToChampions', 'magicDamageDealtToChampions', 'physicalDamageDealtToChampions', 'trueDamageDealtToChampions', 
     'totalHeal', 'totalUnitsHealed', 'damageSelfMitigated', 'damageDealtToObjectives', 'damageDealtToTurrets', 'visionScore', 'timeCCingOthers', 
     'totalDamageTaken', 'magicalDamageTaken', 'physicalDamageTaken', 'trueDamageTaken', 'goldEarned', 'goldSpent', 'turretKills', 'inhibitorKills',
      'totalMinionsKilled', 'neutralMinionsKilled', 'neutralMinionsKilledTeamJungle', 'neutralMinionsKilledEnemyJungle', 'totalTimeCrowdControlDealt',
       'champLevel', 'visionWardsBoughtInGame', 'sightWardsBoughtInGame', 'wardsPlaced', 'wardsKilled', 'firstBloodKill', 'firstTowerKill', 
       'firstTowerAssist', 'firstInhibitorKill', 'firstInhibitorAssist']

#     print(an)
    # stats_p = [an[i] for i in an[2:61] if i!= 'firstBloodAssist']

    stats_p = []
    for k in temp_d:
       
        if k not in an:
            stats_p.append(0)
        else:
            stats_p.append(an[k])

    M += [an['win']]
    M+= [an['participantId']]

#     print(stats_p)
    M += stats_p

    ap10 = jg['frames'][10]['participantFrames'][str(j+1)]
    ap15 = jg['frames'][15]['participantFrames'][str(j+1)]
    ap20 = jg['frames'][20]['participantFrames'][str(j+1)]


    M += [ap10['totalGold'],ap10['level'],ap10['xp'],ap10['minionsKilled'] + ap10['jungleMinionsKilled'] ]
    M += [ap15['totalGold'],ap15['level'],ap15['xp'],ap15['minionsKilled'] + ap15['jungleMinionsKilled']]
    M += [ap20['totalGold'],ap20['level'],ap20['xp'],ap20['minionsKilled'] + ap20['jungleMinionsKilled']]




    team_zer = js['teams'][dic[j][0]]

    M+= [team_zer['riftHeraldKills']]
    M+= [team_zer['baronKills']]
    M+= [team_zer['dragonKills']]

    L = bans(js,champ)
    dddd = dic[j][0]
    for i in L[dddd]:
        M.append(i)

    M.append(jg['frames'][-1]['participantFrames'][str(j+1)]['xp'])

    fbij = fb_ij(jg)

    if j+1 in fbij['assistingParticipantIds']:
        catz = True
    else:
        catz = False

    fbt = katz(fbij['timestamp'])

    M.append(catz)
    M.append(fbt)
    M.append(fbVictim(js,jg,champ,j))


    sk_zer = soloKills(js,jg,champ,j)

    M += [sk_zer]
    M+= [killsassists15(js,jg,champ,j)]
    M+= [deaths15(js,jg,champ,j)]
    gt = gametime(seconds_to_min(js['gameDuration']))
    # print(gt)
    # print(js['participants'][j]['stats']['timeCCingOthers'])

    M.append( js['participants'][j]['stats']['timeCCingOthers']/gt)
    M.append(jg['frames'][-1]['participantFrames'][str(j+1)]['totalGold'])



    
    

    



    return(M)



def players_for_one_game(js,jg,champ):
    game = []

    for i in range(0,10):

        game.append(player_j(js,jg,champ,i))


    return(game)


def concat_zer(x,y,z):
    return([x,y,z])

def players_statistics(df,champ):

    gg = ['gameid',
         'patch',
         'team',
         'player',
         'position',
         'side',
         'gameduration','champion',
         'win',
        'participantId',
         'item0',
         'item1',
         'item2',
         'item3',
         'item4',
         'item5',
         'item6',
         'kills',
         'deaths',
         'assists',
         'largestKillingSpree',
         'largestMultiKill',
         'killingSprees',
         'longestTimeSpentLiving',
         'doubleKills',
         'tripleKills',
         'quadraKills',
         'pentaKills',
         'unrealKills',
         'totalDamageDealt',
         'magicDamageDealt',
         'physicalDamageDealt',
         'trueDamageDealt',
         'largestCriticalStrike',
         'totalDamageDealtToChampions',
         'magicDamageDealtToChampions',
         'physicalDamageDealtToChampions',
         'trueDamageDealtToChampions',
         'totalHeal',
         'totalUnitsHealed',
         'damageSelfMitigated',
         'damageDealtToObjectives',
         'damageDealtToTurrets',
         'visionScore',
         'timeCCingOthers',
         'totalDamageTaken',
         'magicalDamageTaken',
         'physicalDamageTaken',
         'trueDamageTaken',
         'goldEarned',
         'goldSpent',
         'turretKills',
         'inhibitorKills',
         'totalMinionsKilled',
         'neutralMinionsKilled',
         'neutralMinionsKilledTeamJungle',
         'neutralMinionsKilledEnemyJungle',
         'totalTimeCrowdControlDealt',
         'champLevel',
         'visionWardsBoughtInGame',
         'sightWardsBoughtInGame',
         'wardsPlaced',
         'wardsKilled',
         'firstBloodKill',
         'firstTowerKill',
         'firstTowerAssist',
         'firstInhibitorKill',
        'firstInhibitorAssist',
         'totalGold10',
         'level10',
         'xp10',
         'cs10',
         'totalGold15',
         'level15',
         'xp15',
         'cs15','totalGold20','level20','xp20','cs20','riftHeraldKills','baronKills','dragonKills','ban1','ban2','ban3','ban4','ban5','lasttrackedXP',
            'firstBloodAssist','firstBloodTime','firstBloodVictim','soloKills','killsAssists15','deaths15','ccTimepm','totalGold']




    df2 = df.copy(deep = True)

    df2['concat'] = df2[['league','game_json','timeline_json']].apply(lambda x : concat_zer(*x),axis = 1)
#     display(df2)

    out_zer = []
    for i in df2['concat']:
        Mz = players_for_one_game(i[1],i[2],champ)
        

        for k in Mz:
            out_zer.append([i[0]] + k)
 
    return(pd.DataFrame(out_zer,columns = ['league'] + gg))
#     return(out_zer)


def seconds_to_min(k):

    minz = k//60
    secz = k%60


    return(str(minz) + ':' + str(secz))


# ## Analysis Functions

# In[6]:


def kda_(x,y,z):

    if y == 0:
        y1 = 1
    else:
        y1 = y

    return((x+z)/y1)

def sum_fast(x,y):
    return(x+y)


def kda(player,gameid,team,player_s):
    # print(player_s.columns)
    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]

    v = kda_(df_work['kills'].values[0],df_work['deaths'].values[0],df_work['assists'].values[0])


    return(v)
def kp(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team']==team]
    all_k = df_work['kills'].sum()
    # display(df_work)
    p = df_work[df_work['player'] == player]['kills'].values[0] + df_work[df_work['player'] == player]['assists'].values[0]
    return(100*p/all_k)


def dpm(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team']==team][player_s['player'] == player]

    mins = gametime(df_work['gameduration'].values[0])

    dmg = df_work['totalDamageDealtToChampions'].values[0]

    return(dmg/mins)


def gametime(x):
    L = x.split(':')
    if int(L[1])>=50:
        return(int(L[0])+1)
    else:
        return(int(L[0]))


def dmgshare(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team']==team]
    all_dmg = df_work['totalDamageDealtToChampions'].sum()
    df_ = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]

    dmg_ = df_['totalDamageDealtToChampions'].values[0]
    return(100*dmg_/all_dmg)


def cspm(player,gameid,team,player_s):

    df_work = player_s[player_s['gameid'] == gameid][player_s['team']==team][player_s['player'] == player]

    mins = gametime(df_work['gameduration'].values[0])

    total_cs = df_work['totalMinionsKilled'].values[0] + df_work['neutralMinionsKilled'].values[0]

    return(total_cs/mins)



def vspm(player,gameid,team,player_s):

    df_work = player_s[player_s['gameid'] == gameid][player_s['team']==team][player_s['player'] == player]

    mins = gametime(df_work['gameduration'].values[0])

    total_cs = df_work['visionScore'].values[0]

    return(total_cs/mins)


def xpm(player,gameid,team,player_s):

    df_work = player_s[player_s['gameid'] == gameid][player_s['team']==team][player_s['player'] == player]

    mins = gametime(df_work['gameduration'].values[0])

    total_xp = df_work['lasttrackedXP'].values[0]

    return(total_xp/mins)


def fbAssistance(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team']==team][player_s['player'] == player]

    m = df_work['firstBloodKill'].values[0] + df_work['firstBloodAssist'].values[0]

    return(int(m))

def ftAssistance(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team']==team][player_s['player'] == player]

    m = df_work['firstTowerKill'].values[0] + df_work['firstTowerAssist'].values[0]

    return(int(m))


def XPD15(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]
    df_zer = player_s[player_s['gameid'] == gameid]



    id_ = df_work['participantId'].values[0]

    if gameid not in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:9,5:10}
        ccid = {6:1,7:2,8:3,9:4,10:5}

    if gameid in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:10,5:9}
        ccid = {6:1,7:2,8:3,9:5,10:4}

    if id_<=5:
        opp_id = dicc[id_]

    else:
        opp_id = ccid[id_]

    xpplayer = df_zer[df_zer['participantId'] == id_]['xp15'].values[0]
    xpp_opp =  df_zer[df_zer['participantId'] == opp_id]['xp15'].values[0]


    return(xpplayer - xpp_opp)


def GD15(player,gameid,team,player_s):

    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]
    df_zer = player_s[player_s['gameid'] == gameid]



    id_ = df_work['participantId'].values[0]

    if gameid not in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:9,5:10}
        ccid = {6:1,7:2,8:3,9:4,10:5}

    if gameid in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:10,5:9}
        ccid = {6:1,7:2,8:3,9:5,10:4}

    if id_<=5:
        opp_id = dicc[id_]

    else:
        opp_id = ccid[id_]

    xpplayer = df_zer[df_zer['participantId'] == id_]['totalGold15'].values[0]
    xpp_opp =  df_zer[df_zer['participantId'] == opp_id]['totalGold15'].values[0]


    return(xpplayer - xpp_opp)

def GD20(player,gameid,team,player_s):

    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]
    df_zer = player_s[player_s['gameid'] == gameid]



    id_ = df_work['participantId'].values[0]

    if gameid not in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:9,5:10}
        ccid = {6:1,7:2,8:3,9:4,10:5}

    if gameid in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:10,5:9}
        ccid = {6:1,7:2,8:3,9:5,10:4}

    if id_<=5:
        opp_id = dicc[id_]

    else:
        opp_id = ccid[id_]

    xpplayer = df_zer[df_zer['participantId'] == id_]['totalGold20'].values[0]
    xpp_opp =  df_zer[df_zer['participantId'] == opp_id]['totalGold20'].values[0]


    return(xpplayer - xpp_opp)


def deathshare(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]
    df_zer = player_s[player_s['gameid'] == gameid][player_s['team'] == team]
    id_ = df_work['deaths'].values[0]

    vals= df_zer['deaths'].sum()
    if vals == 0:
        vals =1
    return(100*id_/vals)


def goldshare(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]
    df_zer = player_s[player_s['gameid'] == gameid][player_s['team'] == team]
    id_ = df_work['totalGold'].values[0]

    vals = df_zer['totalGold'].sum()
    if vals == 0:
        vals =1
    return(100*id_/vals)




def CSD15(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]
    df_zer = player_s[player_s['gameid'] == gameid]





    id_ = df_work['participantId'].values[0]

    if gameid not in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:9,5:10}
        ccid = {6:1,7:2,8:3,9:4,10:5}

    if gameid in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:10,5:9}
        ccid = {6:1,7:2,8:3,9:5,10:4}



    
    if id_<=5:
        opp_id = dicc[id_]

    else:
        opp_id = ccid[id_]

    xpplayer = df_zer[df_zer['participantId'] == id_]['cs15'].values[0]
    xpp_opp =  df_zer[df_zer['participantId'] == opp_id]['cs15'].values[0]


    return(xpplayer - xpp_opp)


def dragonshare(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]
    df_zer = player_s[player_s['gameid'] == gameid]



    id_ = df_work['participantId'].values[0]

    if gameid not in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:9,5:10}
        ccid = {6:1,7:2,8:3,9:4,10:5}

    if gameid in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:10,5:9}
        ccid = {6:1,7:2,8:3,9:5,10:4}



    if id_<=5:
        opp_id = dicc[id_]

    else:
        opp_id = ccid[id_]

    nb_drag = df_zer[df_zer['participantId'] == id_]['dragonKills'].values[0]
    nb_drag_op =  df_zer[df_zer['participantId'] == opp_id]['dragonKills'].values[0]



    return(100*nb_drag/(nb_drag + nb_drag_op))

def baronshare(player,gameid,team,player_s):
    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]
    df_zer = player_s[player_s['gameid'] == gameid]



    id_ = df_work['participantId'].values[0]

    if gameid not in [1150374,1160095,1160590,1170241]:
        dicc =  {1:6,2:7,3:8,4:9,5:10}
        ccid = {6:1,7:2,8:3,9:4,10:5}

    if gameid in [1150374,1160095,1160590,1170241]:
        dicc = {1:6,2:7,3:8,4:10,5:9}
        ccid = {6:1,7:2,8:3,9:5,10:4}


    if id_<=5:
        opp_id = dicc[id_]

    else:
        opp_id = ccid[id_]

    nb_drag = df_zer[df_zer['participantId'] == id_]['baronKills'].values[0]
    nb_drag_op =  df_zer[df_zer['participantId'] == opp_id]['baronKills'].values[0]



    return(100*nb_drag/(nb_drag + nb_drag_op))

def counterjglshare(player,gameid,team,player_s):

    df_work = player_s[player_s['gameid'] == gameid][player_s['team'] == team][player_s['player'] == player]

    total_cs = df_work['totalMinionsKilled'].values[0] + df_work['neutralMinionsKilled'].values[0]

    bep = df_work['neutralMinionsKilledEnemyJungle'].values[0]

    return(100*bep/total_cs)


def get_player_index_clean(df):

    i = df['player'].unique().tolist()
    out = []

    for j in i:

        team = df[df['player'] == j]['team'].values[0]
        league = df[df['player'] == j ]['league'].values[0]
        posi = list(df[df['player'] == j]['position'].unique())

        out.append([league,team,j,posi])

    return(pd.DataFrame(out,columns = ['league','team','player','position']))

# kda,kp,dpm,dmgshare,cspm,xpm,fbassistance,ftassistance,xpd15,csd15,dragonshare,baronshare


# ## Final Dataset Functions
#
#

# In[416]:


def get_seconds(x):
    d = x.split(':')

    return(60*int(d[0])+int(d[1]))

def concat_5(a,b,c,d,e):
    return([a,b,c,d,e])

def count_pos(bans,players,champ):
    m = [0,0,0,0,0]
    champ_ = ['','','','','']
    for p in bans:
        if p != 'No Ban':
            
            pos = champ[champ['name'] == p]['main_position'].values

            if 'Top' in pos:
                m[0]+=1
                champ_[0]+=' ' + p
            if 'Jungle' in pos:
                m[1]+=1
                champ_[1]+=' ' + p
            if 'Mid' in pos:
                m[2]+=1
                champ_[2]+=' ' + p
            if 'ADC' in pos:
                m[3]+=1
                champ_[3]+=' ' + p
            if 'Support' in pos:
                m[4]+=1
                champ_[4]+=' ' + p


            pos2 = champ[champ['name'] == p]['second_position'].values[0]
            if pos2 == pos2:
                if 'Top' in pos2:
                    m[0]+=1/2
                    champ_[0]+=' ' + p
                if 'Jungle' in pos2:
                    m[1]+=1/2
                    champ_[1]+=' ' + p
                if 'Mid' in pos2:
                    m[2]+=1/2
                    champ_[2]+=' ' + p
                if 'ADC' in pos2:
                    m[3]+=1/2
                    champ_[3]+=' ' + p
                if 'Support' in pos2:
                    m[4]+=1/2
                    champ_[4]+=' ' + p


    fine = [['Top',players[0],m[0],champ_[0]],['Jungle',players[1],m[1],champ_[1]],['Middle',players[2],m[2],champ_[2]],['ADC',players[3],m[3],champ_[3]],['Support',players[4],m[4],champ_[4]]]

    return(fine)


def count_bans(bans,player,position,champ):

    dicc = {'Top':[player,'','','',''],'Jungle':['',player,'','',''],'Mid':['','',player,'',''],'ADC':['','','',player,''],
           'Support':['','','','',player]}

    ccid = {'Top':0,'Jungle':1,'Mid':2,'ADC':3,'Support':4}

    fine = count_pos(bans,dicc[position],champ)

    return(fine[ccid[position]])


def split_add(x):

    cham = x.split(' ')

    return([i for i in cham if i!=' ' and i!= ''])


def ban_score(df,player,position,champ):
    M = []
    df_w = df[df['player'] == player][['ban1','ban2','ban3','ban4','ban5']].copy(deep = True)
    df_w['ban_concat'] = df_w[['ban1','ban2','ban3','ban4','ban5']].apply(lambda x : concat_5(*x),axis=1)
    df_z = df_w['ban_concat']
    for i in df_z:
#         print(i)
        M.append(count_bans(i,player,position,champ))

    dv = pd.DataFrame(M,columns = ['position','player','ban_score','champs'])

    moy = dv['ban_score'].mean()

    dv['output'] = dv['champs'].apply(split_add)
    champs = []
    for j in dv['output']:
        for k in j:
            if k not in champs:
                champs.append(k)

    return([moy,champs,len(champs)])


def unique_champions(df1,df2,player):

    dz = df2[df2['player'] == player]['position'].values[0]

    dk = len(df1[df1['player'] == player])
#     print(dk)
    M = [player]
    for i in range(0,len(dz)):
#         print(dz)
        df_work = df1[df1['player']==player][df1['position'] == dz[i]]
        champs = list(df_work[df_work['win'] == True]['champion'].unique())
        M.append([dz[i],champs,len(champs)])


    while len(M) < 4:
        M.append([' ', [],0])

#     print([M])
    L = []
    for j in M:
#         print(type(j))
        if type(j) != list:
            L.append(j)
        else:
            for k in j:
                L.append(k)

    return(L)








# In[444]:


def trolling(df,champ):
    df['trolling'] = False


    unique_game_ids = df['gameid'].unique().tolist()



    for j in unique_game_ids:
        if check_if_troll(df[df['gameid'] == j][['team','player','position','champion','participantId']],champ) == True:
            df.loc[df['gameid'] == j,'trolling'] = True

def most_played_position(df_work):

    unique_players = list(df_work['player'].unique())

    player_team_position = []
    for i in unique_players:
        pos = list(df_work[df_work['player'] == i]['position'].value_counts().index[0:])
        team = df_work[df_work['player'] == i]['team'].values[0]
        player_team_position.append([team,i,pos,len(pos)])


    return(pd.DataFrame(player_team_position,columns = ['team','player','positions','size']))

def check_if_troll(df,champ):
    m = 0
#     display(df)
    for j in range(1,11):

        chp = df[df['participantId'] == j]['champion'].values[0]
        pos = df[df['participantId'] == j]['position'].values[0]
        if pos not in [champ[champ['name'] == chp]['main_position'].values[0],champ[champ['name'] == chp]['second_position'].values[0],champ[champ['name'] == chp]['third_position'].values[0]]:
            m+=1

#     print(m)
    if m>=3:
        return True
    else:
        return False



def check_pos(df,df_ptp):
    m = 0
    for j in range(1,11):
        nameuh = df[df['participantId'] == j]['player'].values[0]
        pos = df[df['participantId'] == j]['position'].values[0]

        main_pos = df_ptp[df_ptp['player'] == nameuh]['positions'].values[0][0]

        if main_pos != pos:
            # print(nameuh)
            # print(main_pos)
            return False
        else:
            True

    return True


def get_right_df(df_work_):
    df_work = df_work_[df_work_['trolling'] == False]
    df_ptp = most_played_position(df_work)
    unique_players = df_work['player'].unique().tolist()


    player = unique_players[0]




    one_role_played = df_ptp[df_ptp['size'] == 1]['player'].tolist()

    pos = df_ptp[df_ptp['player'] == one_role_played[0]]['positions'].values[0][0]

    second_role_played = df_ptp[df_ptp['size'] > 1]['player'].tolist()


    # sndrole = df_ptp[df_ptp['player'] == second_role_played[0]]['positions'].values[0][1]


    main_pos_df = df_work[df_work['player'].isin(one_role_played)].copy(deep = True)

    second_pos_df = df_work[df_work['player'] == 'rehareha_ramanana'][df_work['position'] == 'reformed_flamer'].copy(deep = True)


    for k in second_role_played:

        if k not in main_pos_df['player'].tolist():
            pos1 = df_ptp[df_ptp['player'] == k]['positions'].values[0][0]
            main_pos_df = main_pos_df.append(df_work[df_work['player'] == k][df_work['position'] == pos1])


        pos2 = df_ptp[df_ptp['player'] == k]['positions'].values[0][1]

        second_pos_df = second_pos_df.append(df_work[df_work['player'] == k][df_work['position'] == pos2])

    return([main_pos_df,second_pos_df])




def get_analysis(df_work):
    df_zer = df_work.copy(deep = True)
    M = [['kda1',kda],['kp1',kp],['dpm',dpm],['dmg%',dmgshare],['cspm',cspm],['xpm',xpm],['fbassistance',fbAssistance],
         ['ftAssistance',ftAssistance],['XPD15',XPD15],['CSD15',CSD15],['Dr%',dragonshare],['Bar%',baronshare],['counterjgl%',counterjglshare],
         ['death%',deathshare],['GD@15',GD15],['GD@20',GD20],['vspm',vspm],['gold%',goldshare]]


    for i in M:

        name = i[0]
        f = i[1]


        df_work[name] = df_work[['player','gameid','team']].apply(lambda x : f(*x,df_zer),axis = 1)





def col_special(df_work):
    ##kda
    df_zer = df_work[['player','kills','deaths','assists']].groupby('player').sum().reset_index()
    df_zer['kda'] = df_zer[['kills','deaths','assists']].apply(lambda x : kda_(*x),axis = 1)

    ##kp
    kp = []
    for j in df_zer['player']:

        gameids = df_work[df_work['player'] == j]['gameid'].tolist()

        kills_player = df_work[df_work['player'] == j][df_work['gameid'].isin(gameids)]['kills'].sum()
        assists_player = df_work[df_work['player'] == j][df_work['gameid'].isin(gameids)]['assists'].sum()

        kills_team = 0
        for k in gameids:

            team = df_work[df_work['player'] == j][df_work['gameid'] == k]['team'].values[0]
            kills_team+= (df_work[df_work['gameid'] ==k][df_work['team'] == team]['kills'].sum())


        if kills_team == 0:
            kills_team = 1

        kp.append([j,100*(kills_player+assists_player)/kills_team])


    df_2 = pd.DataFrame(kp,columns = ['player','kp%'])

    df_fin = df_zer[['player','kda']].merge(df_2,left_on = 'player',right_on = 'player')

    return(df_fin)

def fbshare(df_work):
    df_zer = df_work[['player','kills','deaths','assists']].groupby('player').sum().reset_index()
    df_pute = df_work.copy(deep = True)
    df_pute['fbAssistance'] = df_work['firstBloodKill'] + df_work['firstBloodAssist']
    fb = []
    for j in df_zer['player']:

        dth = len(df_pute[df_pute['player'] == j][df_pute['fbAssistance'] == True])
        tot = len(df_work[df_work['player'] == j])
        if tot == 0:
            tot =1
        fb.append([j,100*dth/tot])

    return(pd.DataFrame(fb,columns = ['player','fb%']))

def victimshare(df_work):
    df_zer = df_work[['player','kills','deaths','assists']].groupby('player').sum().reset_index()
    fb = []
    for j in df_zer['player']:

        dth = len(df_work[df_work['player'] == j][df_work['firstBloodVictim'] == True])
        tot = len(df_work[df_work['player'] == j])
        if tot == 0:
            tot =1
        fb.append([j,100*dth/tot])

    return(pd.DataFrame(fb,columns = ['player','fbVictim%']))


def deathsharetot(df_work):
    df_zer = df_work[['player','kills','deaths','assists']].groupby('player').sum().reset_index()


    death = []
    for j in df_zer['player']:

        gameids = df_work[df_work['player'] == j]['gameid'].tolist()

        deaths_player = df_work[df_work['player'] == j][df_work['gameid'].isin(gameids)]['deaths'].sum()

        deaths_team = 0
        for k in gameids:

            team = df_work[df_work['player'] == j][df_work['gameid'] == k]['team'].values[0]
            deaths_team += (df_work[df_work['gameid'] ==k][df_work['team'] == team]['deaths'].sum())


        if deaths_team == 0:
            deaths_team = 1

        death.append([j,100*deaths_player/deaths_team])

    df_2 = pd.DataFrame(death,columns = ['player','death%'])
    return(df_2)


def df_work_overall(df_work_,champ):


    df_work = df_work_[df_work_['trolling'] == False]

    col_not = ['league','team','player']
    col_sum = ['win','kills','deaths','assists','doubleKills','tripleKills','pentaKills','soloKills']
    col_special_ = ['kda','kp','death%']
    col_avg = ['gameduration','visionScore','timeCCingOthers','totalDamageTaken','turretKills','inhibitorKills',
              'visionWardsBoughtInGame','wardsPlaced','wardsKilled','totalGold15','level10','cs10','dpm','dmg%',
              'cspm','xpm','ftAssistance','XPD15','CSD15','Dr%','Bar%','counterjgl%','killsAssists15','deaths15','GD@15','vspm','ccTimepm','riftHeraldKills','gold%']

    df_work_sum = df_work.groupby('player')[col_sum].sum().reset_index()
    df_work_not = get_player_index_clean(df_work)
    df_work_avg = df_work[['player'] + col_avg].groupby('player').mean().reset_index()

    df_work_overall = df_work_not.merge(df_work_sum,left_on = 'player',right_on = 'player').merge(df_work_avg,left_on = 'player',right_on = 'player')

    df_work_overall['main_position'] = df_work_overall['position'].apply(lambda x : x[0])

    df_work_overall['ban_score'] = df_work_overall[['player','main_position']].apply(lambda x : ban_score(df_work,*x,champ)[0],axis=1)

    df_work_overall['champions_banned'] = df_work_overall[['player','main_position']].apply(lambda x : ban_score(df_work,*x,champ)[2],axis=1)


    M = []

    for i in df_work_overall['player']:

        M.append(unique_champions(df_work,df_work_overall,i) + [len(df_work[df_work['player'] == i])])

    ded = pd.DataFrame(M,columns = ['player','pos1','pos1champs','pos1uniquescore',
                                    'pos2','pos2champs','pos2uniquescore',
                                    'pos3','pos3champs','pos3uniquescore','gp'])


    df_work_overall = df_work_overall.merge(ded,left_on = 'player',right_on = 'player')

    deaths = deathsharetot(df_work)

    col_special_ = col_special(df_work)

    fbvic = victimshare(df_work)
    fb = fbshare(df_work)
    col_special_ = fbvic.merge(col_special_,left_on = 'player',right_on = 'player').merge(fb,left_on = 'player',right_on = 'player')

    # return(col_special_)

    df_special = deaths.merge(col_special_,left_on = 'player',right_on = 'player')

    # return(df_special)

    df_work_overall = df_work_overall.merge(df_special,left_on = 'player',right_on = 'player')

    df_work_overall['efficiency'] = df_work_overall['dmg%']/df_work_overall['gold%']

    return(df_work_overall)


def df_final(df_work,champ):
    trolling(df_work,champ)
    get_analysis(df_work)

    df1,df2 = get_right_df(df_work)

    df1_ = df_work_overall(df1,champ)
    if len(df2) != 0:
        df2_ = df_work_overall(df2,champ)
    else:
        df2_ = df1_.copy(deep = True)

    cols =  ['player','pos1','pos1champs','pos1uniquescore',
                                    'pos2','pos2champs','pos2uniquescore',
                                    'pos3','pos3champs','pos3uniquescore']

    df2_ = df2_[['player'] + [i for i in df2_.columns.tolist() if i not in cols]].merge(df1_[cols],left_on = 'player',right_on = 'player')


    return([df1_,df2_])


# ## Classes

# In[437]:


class League:

    def __init__(self,name,url_gp,champ):
        self.name = name

        self.url_gp = url_gp
        self.champ = champ
        self.Team_B = False
        self.Player_B = False
        self.DF_B = False


    def get_df(self):

        ans = []
        html_page = urllib.request.urlopen(self.url_gp)
        soup = bs4.BeautifulSoup(html_page,"lxml")
        for link in soup.findAll('a', attrs={'href': re.compile("matchhistory")}):

            zz = link.get('href')
            if 'gameHash' in zz:
                if zz not in ans:
                    ans.append(zz)

        ans_2 = [[self.name,i] for i in ans]
        df_mh = pd.DataFrame(ans_2,columns = ['league','mh'])

        df_mh['url_game'] = df_mh['mh'].apply(lambda x : json_game(x))
        df_mh['url_timeline'] = df_mh['mh'].apply(lambda x : json_timeline(x))

        cookie = { '__cfduid':'d804c965683c8f4a98c18ad1372ad87561564848776', '_gcl_au':'1.1.1015131273.1564845192', '_ga':'GA1.2.1020982244.1564845192', '_scid':'d8e0471f-aa41-4475-9032-d72c0ec51ef6', 'C3UID-694':'11815915641564845206', 'C3UID':'11815915641564845206', '_fbp':'fb.1.1564845206149.1012397235', 's_fid':'00A319F9D3AC2E61-3B5334B918D59D8E', '_tlp':'2820:16705877', 'ajs_group_id':'null', '_hjid':'e5df7bce-6d52-4c8c-ba60-c545191cf9de', 'ajs_user_id':'null', 'new_visitor':'false', 'PVPNET_REGION':'euw', 'ping_session_id':'cf0345d8-b9c4-49b6-adbf-ac53b92a8294', '_gid':'GA1.2.1541849144.1577796316', 'PVPNET_TOKEN_EUW':'eyJkYXRlX3RpbWUiOjE1Nzc3OTYzMzAzNzAsImdhc19hY2NvdW50X2lkIjoyMDYzNTQxMDMsInB2cG5ldF9hY2NvdW50X2lkIjoyMDYzNTQxMDMsInN1bW1vbmVyX25hbWUiOiJVbkdyYW5kRHVUaWVrcyIsInZvdWNoaW5nX2tleV9pZCI6IjkwMzQ3NTJiMmI0NTYwNDRhZTg3ZjI1OTgyZGFkMDdkIiwic2lnbmF0dXJlIjoiQnB5VENOcHNSRzI1M3REbXY2RUI4eTQ3b1U1RWZEbG1pVW9PWU1WWkxlNDhwL3FmV3BCV011aDB3U1dNVzlyaEtCczVyV1hoRDR5ek4yWnltQm9GeitqK0ZNc1N6dm5qc2MxR0pDaHlHSGxGckkrbnA2b2lBZ3U1dTJNWGtBSGMrc0dPY1ZIOFczS1lhNjJzRnpxUzRIcmtZb1d6WUR4YkF1eDRZTVlGdE1JPSJ9', 'PVPNET_ACCT_EUW':'UnGrandDuTieks', 'PVPNET_ID_EUW':'206354103', 'PVPNET_LANG':'en_US', 'id_token':'eyJraWQiOiJzMSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiI4NmUzMjViMy1iYzdiLTU0NTMtYmE1OC01OGJiM2M5YjU0ODEiLCJjb3VudHJ5IjoiZ2JyIiwiYW1yIjpbImNvb2tpZSJdLCJpc3MiOiJodHRwczpcL1wvYXV0aC5yaW90Z2FtZXMuY29tIiwibG9sIjpbeyJjdWlkIjoyMDYzNTQxMDMsImNwaWQiOiJFVVcxIiwidWlkIjoyMDYzNTQxMDMsInVuYW1lIjoiYmlnMjFyYXkiLCJwdHJpZCI6bnVsbCwicGlkIjoiRVVXMSIsInN0YXRlIjoiRU5BQkxFRCJ9XSwibG9jYWxlIjoiZW5fVVMiLCJhdWQiOiJyc28td2ViLWNsaWVudC1wcm9kIiwiYWNyIjoiMCIsImV4cCI6MTU3Nzg4MjcyOSwiaWF0IjoxNTc3Nzk2MzI5LCJhY2N0Ijp7ImdhbWVfbmFtZSI6IlVuR3JhbmREdVRpZWtzIiwidGFnX2xpbmUiOiJFVVcifSwianRpIjoieU12RC1BYVE0SzQiLCJsb2dpbl9jb3VudHJ5IjoibXl0In0.HQSi5gkZ3nuuCrarU3RxUp3hKBq9JRlVhcU6y2_wO0Wi9BcaHVKfYr7zq5VTxQXIYEOcTwONkbNixuTamMdGEYAfc3J269BZqiJ1JBF9F5w-HvvN3jqa3AGmdDsGLQuBwK7k6zBlgDkT6Ro14msRZ5eHt_Lu9xunIf5HmNlxEgY', 'id_hint':'sub%3D86e325b3-bc7b-5453-ba58-58bb3c9b5481%26lang%3Den%26game_name%3DUnGrandDuTieks%26tag_line%3DEUW%26id%3D206354103%26summoner%3DUnGrandDuTieks%26region%3DEUW1%26tag%3Deuw', 's_cc':'true', 's_sq':'%5B%5BB%5D%5D', 's_nr':'1577809059695-Repeat', 'rp2':'1577809059696-Repeat', 's_ppv':'lol2%253Aacs%253Aen%253A500%2520error%2C100%2C84%2C800', '_tlc':':1577809069:acs.leagueoflegends.com%2F:leagueoflegends.com', '_tlv':'11.1565040204.1577797302.1577809069.18.1.1', '_sctr':'1|1577750400000', '_gat':'1'}
        df_mh['game_json']=df_mh['url_game'].apply(lambda x : get_request(x,cookie))

        df_mh['timeline_json']= df_mh['url_timeline'].apply(lambda x : get_request(x,cookie))



        # df_mh['matchups'] = df_mh['game_json'].apply(lambda js :    (js['participantIdentities'][0]['player']['summonerName'].split(' ')[0] + ' vs ' +
        # js['participantIdentities'][5]['player']['summonerName'].split(' ')[0]))
        # df_mh['blueside'] = df_mh['game_json'].apply(lambda js :    (js['participantIdentities'][0]['player']['summonerName'].split(' ')[0]))
        # df_mh['redside'] = df_mh['game_json'].apply(lambda js :     js['participantIdentities'][5]['player']['summonerName'].split(' ')[0])


        self.df = df_mh
        self.DF_B = True




    def get_team_stats(self):



        self.team_statistics = team_statistics(self.df,self.champ)
        self.Team_B = True


    def get_player_stats(self):
        self.player_statistics = players_statistics(self.df,self.champ)
        self.Player_B = True




class Leagues:

    def __init__(self,name,url_gp,url_bp,champ,df = None,checkz = False):
        if checkz == False:
            self.url_bp = url_bp
            self.name = name
            self.url_gp = url_gp

            self.champ = champ

            self.n = len(url_gp)

            self.check = (len(name) == len(url_gp))
            self.DF_B = False
            self.Player_B = False
            self.Team_B = False
        else:
            self.name = name
            self.url_bp = url_bp
            self.url_gp = url_gp
            self.champ = champ
            self.n = len(url_gp)
            self.check = (len(name) == len(url_gp))

            if type(df) != pd.core.frame.DataFrame:

                df_w = df[0].copy(deep=True)

                for j in range(1,len(df)):
                    df_w = df_w.append(df[j])

                self.df = df_w
            else:


                col_z = [i for i in list(df.columns) if 'Unnamed' not in i]
                self.df = df[col_z]

            self.DF_B = True
            self.Player_B = False
            self.Team_B = False


    def get_df(self):

        leagues = []
        df_z = []
        if self.check:
            for i in range(0,self.n):
                leagues.append(League(self.name[i],self.url_gp[i],self.champ))

            for i in leagues:
                i.get_df()

                df_z.append(i.df)


        df_w = df_z[0].copy(deep=True)

        for j in range(1,len(df_z)):
            df_w = df_w.append(df_z[j])

        self.df = df_w
        display(self.df)
        self.df['matchups'] = self.df['game_json'].apply(lambda js :    (js['participantIdentities'][0]['player']['summonerName'].split(' ')[0] + ' vs ' +
    js['participantIdentities'][5]['player']['summonerName'].split(' ')[0]))
        self.df['redside'] = self.df['matchups'].apply(lambda x : x.split(' vs ')[1])
        self.df['blueside'] = self.df['matchups'].apply(lambda x : x.split(' vs ')[0])

        self.DF_B = True


    def get_team_stats(self):



        self.team_statistics = team_statistics(self.df,self.champ)
        df_ = ban_phase(self.url_bp)

        self.team_statistics = self.team_statistics.merge(df_.loc[df_.index.repeat(2)].reset_index(drop = True)[['Phase', 'Blue', 'Red', 'Score', 'Patch', 'BB1', 'RB1', 'BB2', 'RB2',
       'BB3', 'RB3', 'BP1', 'RP1-2', 'BP2-3', 'RP3', 'RB4', 'BB4', 'RB5',
       'BB5', 'RP4', 'BP4-5', 'RP5']],left_index = True,right_index = True)
        self.Team_B = True


    def get_player_stats(self):


        self.player_statistics = players_statistics(self.df,self.champ)

        ps =  self.player_statistics
        ps['patch'] = ps['patch'].apply(lambda x : float(x))

        indexes_ad = ps[ps['champion'] == 'Senna'][ps['patch'] >= 10.3][ps['position'] != 'Support'].index
        indexes_support = ps[ps['champion'] == 'Senna'][ps['patch'] >= 10.3][ps['position'] != 'Support'].index +1
        for i in i_s:
            ps.loc[i,'position'] = 'Support'
            ps.loc[i+1,'position'] = 'ADC'
        self.player_statistics = ps

        self.Player_B = True



    def get_final_player(self):
        if self.Player_B == False:
            if self.DF_B == False:
                get_df()
            self.player_statistics = players_statistics(self.df,self.champ)


        self.final_player = df_final(self.player_statistics,self.champ)

    def get_final_team(self):
        if self.Team_B == False:
            if self.DF_B == False:
                get_df()
            self.get_team_stats()
        
        self.final_team = get_team(self.team_statistics,self.df)



# In[463]:


# Name Cleaning ERL
def clean_erl(df_work):

    df_work[df_work['player'].isin(['Rafa','RafaL0L','DûaLL'])]
    more_life = ['XL','ΧL']
    pn = ['p1noyszn','P1noy']

    cbono = ['Carb0no','Carbon0','Carbono','Carbonô']
    tynx = ['TynX','Tynx']
    rf = ['Rafa','RafaL0L']
    dl = ['DuaLL','DuαLL','DûaLL']
    flk = ['Flakked','Flaκκed',
     'Flaккed',
     'Flâkked']
    flx = ['Flaxxish','flaxxish']
    hrt = ['HiRit1', 'HiRit',
     'HiRit1',
     'HiRÍt']
    xxo = ['Raxx0','Raxxo']
    HP = ['HungryPanDa',
     'HungryPanda']
    hstl =  ['HustlinBeasT',
     'HustlinbeasT',]

    konektiv = ['KONEKTiV','KONEKTIV','Konektiv']
    kd = ['KoIdo','Koldo']
    pks = ['PUKI STYLE','Puki Style',
     'Puki style']

    sb = ['SteeelbackR','SteelBackR','SteeelBackR']
    xd = ['XDSMILEY','XDSM1LEY']
    lb = ['labr0v',
     'labrov']


    bc = ['Baca','BÂCA','Bâca']
    ckr = ['Cinkrof','Cinkrof1']
    xile = ['EXILE','Exile']
    jv = ['JaVaaa','JáVaaa']
    ld = ['LIDER','LlDER']
    Lck = ['Lucker','Lucker3']
    pp = ['PropaPandah','PropaMeal']
    shl = ['Shlatan','ShIatan']

    dezsty = ['styIIEE','StyllEE']

    agr = ['Agresivo','Agresivooo']
    clean_names = [cbono,tynx,rf,dl,flk,flx,hrt,xxo,HP,hstl,konektiv,kd,pks,sb,xd,lb,pn,bc,
                  xile,jv,ld,Lck,pp,shl,ckr,agr]

    for i in clean_names:
        org = i[0]

        df_work.loc[df_work['player'].isin(i),'player'] = org


    org = more_life[0]

    df_work.loc[df_work['team'].isin(more_life),'team'] = org
    mora = ['TTQ','TQ']
    org = mora[0]

    df_work.loc[df_work['team'].isin(mora),'team'] = org



def clean_lec(df_work):


    abd = ['Abbedagge','S04 Abbedagge']
    grl = ['GorillA','Gorilla']
    mt = ['Mithy','mithy']
    nd = ['Nukeduck','nukeduck']
    clean_nm = [abd,grl,mt,nd]


    for i in clean_nm:
        org = i[0]

        df_work.loc[df_work['player'].isin(i),'player'] = org

def clean_pos(erl):
    df_try= erl.player_statistics
    df_to_append = df_try[df_try['gameid'] == 1161729][df_try['player'].isin(['Ronaldooo','MagiFelix']) == False]

    df_to_copy1 = df_try[df_try['gameid'] == 1161729][df_try['player'] == 'Ronaldooo']
    df_to_copy1.loc[:,'position'] = 'Mid'
    df_to_copy2 = df_try[df_try['gameid'] == 1161729][df_try['player'] == 'MagiFelix']
    df_to_copy2.loc[:,'position'] = 'Top'

    df_to_copy1 = df_to_copy1.append(df_to_copy2)

    df_to_append = df_to_append.append(df_to_copy1)
    df_to_append

    df_all = df_try[df_try['gameid'] != 1161729]

    df_all = df_all.append(df_to_append)
    return(df_all)
# df_try.loc[df['gameid'] != 1161729]



# TynX,carbono,DûaLL,Flakked, HiRit1,ungryPanDa,HustlinBeasT,KONEKTiV, Koldo,P1noy,
# Puki Style,Rafa,SteelbackR,XDSMILEY,flaxxish,labrov


# In[122]:


def get_nan_df(df):
    # NaN values in df
    nan_values = df.isnull().sum() #counts how many nan values per colus
#     nan_values = nan_values[nan_values > 0] #take the positive one
    nan_values_abs_sort = nan_values.sort_values(ascending=True)

    # number of samples
    M = float(df.shape[0])

    # percent nan_values
    nan_values_per_sort = (nan_values / M).sort_values(ascending=True)
    res = {'abs_nan': nan_values_abs_sort,
           'percent_nan': nan_values_per_sort
          }

    return pd.DataFrame(res) #returns absolute values of number of nans and percentage of nans

def get_cat_feature_counts(df, cat_features):
    res = {}
    for f in cat_features:
        res[f] = len(df[f].unique())
    return res

def nan_rate(df):
    nan_rate = df.isnull().sum()
    return(nan_rate/len(df))

def get_information(df):
    i_ = []
    dtype_ = []
    unique_ = []
    unique_size = []
    nan_rate_ = []
    out = []

    for i in df.columns:
        i_.append(i)
        dtype_.append(df[i].dtype)
        unique_.append(df[i].unique())
        nan_rate_.append(nan_rate(df[i]))
        unique_size.append(len(df[i].unique()))

    for i in range(0,len(i_)):
        out.append([i_[i],dtype_[i],unique_[i],unique_size[i],nan_rate_[i]])


    return(pd.DataFrame(out,columns = ['feature','dtype','unique','unique_size','nan_rate']))


def get_team(df,dto):
    
    dict_add = {'win' : 'wins'}
    dict_avg = {'firstBlood':'fb%','firstTower':'ft%','firstInhibitor':'firstInib%','firstBaron':'firstBaron%',
           'firstDragon':'firstDragon%','firstRiftHerald':'firstRift%','towerKills':'towerKillsPerGame',
            'baronKills':'baronKillsPerGame','dragonKills' : 'dragonKillsPerGame','totalKills':'KillsPerGame','totalDeaths':'DeathsPerGame',
            'totalAssists':'AssistsPerGame','towersLost':'towersLostPerGame','totalWardsPlaced':'WardsPlacedPerGame',
               'totalWardsCleared':'WardsClearedPerGame','totalVisionWardPlaced' : 'VisionWardsPlacedPerGame',
               'totalVisionWardKilled':'VisionWardKilledPerGame','dragons%' : 'AverageDragon%PerGame','baron%':'AverageBaron%PerGame','riftHeraldKills' :'HeraldKillsPerGame'}
    
    
    for k in [j for j in df.columns if '@' in j]:
        dict_avg[k] = 'Average ' + k

    
    col_add = ['win','towerKills','totalKills','totalDeaths','inhibitorKills','baronKills','dragonKills','riftHeraldKills','totalWardsPlaced',
       'totalWardsCleared', 'totalVisionWardPlaced', 'totalVisionWardKilled','totalTimeCrowdControlDealt']

    col_avg = ['firstBlood', 'firstTower', 'firstInhibitor', 'firstBaron','firstDragon', 'firstRiftHerald',
       'towerKills','baronKills','totalKills','dragonKills','riftHeraldKills',
      'totalDeaths','totalAssists', 'towersLost', 'towersKilled@10', 'towersLost@10', 'towersKilled@15',
    'towersLost@15', 'towersKilled@20', 'towersLost@20', 'towersD@10',
    'towersD@15', 'towersD@20', 'dragons@15', 'dragons@20','heralds@10', 'kills@5', 'deaths@5',
    'kills@10', 'deaths@10', 'kills@15', 'deaths@15', 'kills@20',
    'deaths@20','totalWardsPlaced','dragons%','baron%',
    'totalWardsCleared','minutesPlayed','totalVisionWardPlaced', 'totalVisionWardKilled',
    'totalTimeCrowdControlDealtpm','cspm','dpm','vspm','totalGold@15','totalGoldOpp@15','GD@15','totalGold@20','totalGoldOpp@20','GD@20','KillsPerMinute']

    M = []


    for k in df['team'].unique().tolist():
        m = df[df['team'] == k]['gameid'].tolist()


        sum_dragons_opp = df[df['team'] != k][df['gameid'].isin(m)]['dragonKills'].sum()
        sum_dragons = df[df['team'] == k][df['gameid'].isin(m)]['dragonKills'].sum()

        sum_barons_opp = df[df['team'] != k][df['gameid'].isin(m)]['baronKills'].sum()
        sum_barons = df[df['team'] == k][df['gameid'].isin(m)]['baronKills'].sum()

        sum_heralds_opp = df[df['team'] != k][df['gameid'].isin(m)]['riftHeraldKills'].sum()
        sum_heralds = df[df['team'] == k][df['gameid'].isin(m)]['riftHeraldKills'].sum()

        
        dragonshare = 0
        baronshare = 0
        heraldshare = 0
        
        if sum_dragons+sum_dragons_opp != 0:
            dragonshare = sum_dragons/(sum_dragons+sum_dragons_opp)
 
            
        if sum_barons+sum_barons_opp != 0:
            baronshare = sum_barons/(sum_barons+sum_barons_opp)
  
        if sum_heralds + sum_heralds_opp != 0:
            heraldshare = sum_heralds/(sum_heralds + sum_heralds_opp)

        
        
        win_blue = df[df['team'] == k][df['side'] == 'Blue']['win'].replace('0',0).replace('1',1).sum()
        win_red = df[df['team'] == k][df['side'] == 'Red']['win'].replace('0',0).replace('1',1).sum()

        games_blue = len(df[df['team'] == k][df['side'] == 'Blue'])
        games_red = len(df[df['team'] == k][df['side'] == 'Red'])

        if games_blue == 0 :
            games_blue = 1

        if games_red == 0:
            games_red = 1

        winrate = df[df['team'] == k]['win'].replace('0',0).replace('1',1).sum()/len(df[df['team'] == k])


        M.append([k,winrate,dragonshare,heraldshare,baronshare,games_blue,games_red,win_blue/games_blue,win_red/games_red])

        
        
    df_special = pd.DataFrame(M,columns = ['team','winrate','Overall dra%','Overall herald%','Overall baron%','BlueSideGames','RedSideGames','WRBlue','WRRed'])
        
    df_work = df.copy(deep = True)

    df_add = df_work[['team'] + col_add].groupby('team').sum().reset_index()
    df_avg = df_work[['team'] + col_avg].groupby('team').mean().reset_index()
    
    df_add = df_add.rename(columns = dict_add)
    df_avg = df_avg.rename(columns = dict_avg)


    df_end = special_2(df,dto)

    df_add['kd'] = df_add['totalKills']/df_add['totalDeaths']




    return(df_add.merge(df_avg,left_on = 'team',right_on = 'team').merge(df_special,left_on = 'team',right_on = 'team').merge(df_end,left_on = 'team',right_on = 'team'))
        
def side_first_baron(jg):
    for k in jg['frames']:
        for p in k['events']:
            if p['type'] == 'ELITE_MONSTER_KILL' and p['monsterType'] == 'BARON_NASHOR':
                try:
                    t = p['timestamp']
                except:
                    t = None
                    
                try:
                    if p['killerId']>=6:
                        side = 'Red'
                    else:
                        side = 'Blue'
                except:
                    side = None

                return([t,side])
    return([None,None])


def check_og(x,y,z,team):
    print(z)
    
    if z[1] == 'Red' and y == team:
        return(z[0])
    if z[1] == 'Blue' and x == team:
        return(z[0])
    else:
        return None

def first_baron_time(df,team):
    
    df_og = df[df['redside'] == team].append(df[df['blueside'] == team]).reset_index(drop = True)
    
    df_og['first_baron'] = df_og['timeline_json'].apply(lambda x : side_first_baron(x))
    df_og['time'] = df_og[['blueside','redside','first_baron']].apply(lambda x : check_og(*x,team),axis = 1)

    m = df_og['time'].dropna().mean()
    if np.isnan(m):
        m = 1
    p =katz(m)




    return(p)
    

def special_2(team_statistics,dto):
    df = team_statistics.copy(deep=True)
    timezer = ['firstKillTime','firstDeathTime','firstTurretTime','firstTurretLossTime','gameduration']
    pzer = []
    df_end = pd.DataFrame()
    for p in df['team'].unique().tolist():
        df_work = df[df['team'] == p]
        df_work = df_work.fillna('0:0')

        for j in timezer:
            df_work[j] = df_work[j].fillna('0:0').apply(get_seconds)

        df_end = df_end.append(df_work[['team']+timezer])

    df_end = df_end.groupby('team').mean().reset_index()
    for j in timezer:
        df_end[j] = df_end[j].apply(lambda x : katz(x*1000))  
        
        
    df_end = df_end.rename(columns = {'firstKillTime':'AverageFirstKillTime',
                            'firstDeathTime':'AverageFirstDeathsTime','firstTurretTime':'AverageFirstTurretTime',
                            'firstTurretLossTime':'AverageFirstTurretLossTime','gameduration':'AverageGameTime'})
        

    gpm_ = []
    for p in df['team'].unique().tolist():
        df_work = df[df['team'] == p][['team','totalGold','minutesPlayed']]
        minutes = df_work['minutesPlayed'].sum()
        gpm = df_work['totalGold'].sum()/minutes

        gpm_.append([p,gpm])

    df_end = df_end.merge(pd.DataFrame(gpm_,columns = ['team','gpm']),left_on = 'team',right_on = 'team')
    M = []
    for p in df['team'].unique().tolist():
        M.append([p,first_baron_time(dto,p)])


    df_end = df_end.merge(pd.DataFrame(M,columns = ['team','AverageFirstBaronTime']),left_on = 'team',right_on = 'team')


    return(df_end)
        
                    
def firstKilltime(jg):
    
    killsred = []
    killsblue = []
    for j in jg['frames']:
        for b in j['events']:
            if b['type'] == 'CHAMPION_KILL':
                if b['killerId'] > 5:
                    killsred.append([katz(b['timestamp']),'Red'])
                else:
                    killsblue.append([katz(b['timestamp']),'Blue'])
                    

    try:
        outred = killsred[0]
    except:
        outred = None
    
    try:
        outblue = killsblue[0]
    except:
        outblue = None
    
    return([outblue,outred])
                
def firstTurretTime(jg):
    
    killsred = []
    killsblue = []
    for j in jg['frames']:
        for b in j['events']:
            if b['type'] == 'BUILDING_KILL':
                if b['killerId'] > 5:
                    killsred.append([katz(b['timestamp']),'Red'])
                else:
                    killsblue.append([katz(b['timestamp']),'Blue'])
                    

    try:
        outred = killsred[0]
    except:
        outred = None
    
    try:
        outblue = killsblue[0]
    except:
        outblue = None
    
    return([outblue,outred])
    