import cassiopeia as cass
import requests
import pandas as pd
import time


# apikey1 = 'RGAPI-6b604f29-f0fd-484d-a3ca-36e1fe4f9131'
# apikey2 = 'RGAPI-95cbab63-6799-4944-8c92-90f8e527882f'
# apikey3 = 'RGAPI-749c2c93-7a18-4e34-afa8-1d5b42d56816'

# apis = [apikey1,apikey2,apikey3]


def get_ids(name,api):

    url = 'https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-name/'
    
    url  += name + '?' + 'api_key=' + api ;
    return(requests.get(url).json())



def get_match_history(accountId,api):
    ## Récupère les 200 dernières parties
    
    url = 'https://euw1.api.riotgames.com/lol/match/v4/matchlists/by-account/'
    url_1 =url +  accountId + '?queue=420&season=13&endIndex=100&beginIndex=0&api_key=' + api
    url_2 =url +  accountId + '?queue=420&season=13&endIndex=150&beginIndex=100&api_key=' + api

    match = requests.get(url_1).json()
#     match.append(requests.get(url_2).json())
    match2 = requests.get(url_2).json()
    matchs = []
    gameids = []
    for i in match['matches']:
        gameids.append(i['gameId'])
        matchs.append(i)
        
    
    for j in match2['matches']:
        if j['gameId'] not in gameids:
            matchs.append(j)
            gameids.append(j['gameId'])
        
    return([matchs,gameids])
    
    
def get_match_history_name(name,api):
    
    
    acc_id = get_ids(name,api)['accountId']
    match,gids = get_match_history(acc_id,api)
    
    return([match,gids,acc_id])




def get_match_history_names(name,api):
    acc_id = []
    for i in name:
        
        acc_id.append(get_ids(name,api)['accountId'])
        
    gameids = []
    matchs = []
    
    for i in acc_id:
        gameids.append(get_match_history(i,api)[1])
        matchs.append(get_match_history(i,api)[0])
    
    return(matchs,gameids,acc_id)

def most_played_champs_name(name,api,champ):
    
    matchs,gameids,acc_ids = get_match_history_name(name,api)
    
    cops = champ[['name','main_position','second_position','third_position','key']].copy()
    cops['counts'] = 0
    
    for i in matchs:        
        ind = cops[cops['key'] == i['champion']].index    
        cops.loc[ind,'counts'] += 1
    return(cops[cops['counts'] != 0].sort_values('counts',ascending = False)[['name','counts']])


def most_played_champs_names(name,api,champ):
    
    matchs,gameids= get_match_history_names(name,api)
    
    cops = champ[['name','main_position','second_position','third_position','key']].copy()
    cops['counts'] = 0
    
    for i in matchs:        
        ind = cops[cops['key'] == i['champion']].index    
        cops.loc[ind,'counts'] += 1
    return(cops[cops['counts'] != 0].sort_values('counts',ascending = False)[['name','counts']])




def most_played_champs_per_week(name,date_in,date_out,api,champ):
    

    m = mh_per_week(name,date_in,date_out,api)

    cops = champ[['name','main_position','second_position','third_position','key']].copy()
    cops['counts'] = 0
    
    for i in m[0]:        
        ind = cops[cops['key'] == i['champion']].index    
        cops.loc[ind,'counts'] += 1
    return(cops[cops['counts'] != 0].sort_values('counts',ascending = False)[['name','counts']])

def mh_per_week(name,date_in,date_out,api):
    
    all_mh = get_match_history_name(name,api)
    
    gameids = []
    for i in all_mh[0]:        
        if i['timestamp'] >= get_ts_from_date(date_in) and i['timestamp'] <= get_ts_from_date(date_out):
            gameids.append(i)
            
    return([gameids,all_mh[2]])
        

def match_information(match_id,api):

    url =  'https://euw1.api.riotgames.com/lol/match/v4/matches/'
    url += str(match_id) + '?api_key='
    url += api
    return(requests.get(url))
#     4546887877?api_key=RGAPI-2e1ca10d-dc58-40c2-938d-9522ca223edb



def winrate(name,api,champ):
    
    counter = 0
    matchs,gameids,acc_ids = get_match_history_name(name,api)

    alls = []

    for j in matchs:

        pp = match_information(j['gameId'],api).json()
        time.sleep(1.2)
        chp = champ[champ['key'] == j['champion']]['name'].values[0]
        pp = match_information(j['gameId'],api).json()
        time.sleep(1.2)
        chp = champ[champ['key'] == j['champion']]['name'].values[0]

        try:
            pId,teamId = find_participant_id(pp,acc_ids)
            test = True
        except:
            test = None

        if test == None:
            return('ya Erreur')
        else:
            if pp['teams'][teamId]['win'] == 'Win':
                alls.append([chp,1])
            elif pp['teams'][teamId]['win'] == 'Fail':
                alls.append([chp,0])
    
    return(alls)


def winrate_gp_name(name,api,champ):
    
    cc = most_played_champs_name(name,api,champ)

    
    tt = pd.DataFrame(winrate(name,api,champ),columns = ['champion','result'])
    app = []
    for i in tt['champion'].unique():

        n = len(tt[tt['champion'] == i])
        win = tt[tt['champion'] == i]['result'].sum()

        app.append([i,win/n])
        
    ttt = pd.DataFrame(app,columns = ['champion','winrate'])
    return(ttt.merge(cc,left_on = 'champion',right_on = 'name')[['champion','winrate','counts']])


def winrate_gp_custom_name(name,date_in,date_out,api,champ):
    
    cc = most_played_champs_per_week(name,date_in,date_out,api,champ)

    
    tt = pd.DataFrame(winrate_per_week(name,date_in,date_out,api,champ),columns = ['champion','result'])
    app = []
    for i in tt['champion'].unique():

        n = len(tt[tt['champion'] == i])
        win = tt[tt['champion'] == i]['result'].sum()

        app.append([i,100*win/n])
        
    ttt = pd.DataFrame(app,columns = ['champion','winrate'])

    qq = pd.DataFrame(stats_per_champion('KCorp Helaz',[2020,5,25],[2020,5,31],apikey1,champ)).T.reset_index(drop=True).set_index('champion')
    return(ttt.merge(qq,left_on = 'champion',right_on = 'champion')[['champion','winrate','FirstBloodParticipation', 'GamesPlayed', 'killParticipation',
       'visionScorePerMinute', 'visionWardsBoughtInGame','wardsKilledPerMinute', 'wardsPlacedPerMinute']].sort_values('GamesPlayed',ascending = False))



def winrate_per_week(name,date_in,date_out,api,champ):

    m = mh_per_week(name,date_in,date_out,api)
    
    counter = 0

    alls = []

    for j in m[0]:

        pp = match_information(j['gameId'],api).json()
        time.sleep(1.2)
        chp = champ[champ['key'] == j['champion']]['name'].values[0]

        try:
            pId,teamId = find_participant_id(pp,m[1])
            test = True
        except:
            test = None

        if test == None:
            return('ya Erreur')
        else:
            if pp['teams'][teamId]['win'] == 'Win':
                alls.append([chp,1])
            elif pp['teams'][teamId]['win'] == 'Fail':
                alls.append([chp,0])

        
    
    return(alls)

def find_participant_id(pp,acc_id):

    for i in pp['participantIdentities']:
        if i['player']['currentAccountId'] == acc_id or i['player']['accountId'] == acc_id:
            pId = i['participantId']
            test = 1
            if pId <= 5:
                teamId = 0
            else:
                teamId =1

    return(pId,teamId)


def kp(pp,pId,teamId): 
    val = pp['participants'][pId-1]['stats']['kills'] + pp['participants'][pId-1]['stats']['assists']
    
    if teamId == 0:    
        kills = 0
        for i in range(0,5):
            kills += pp['participants'][i]['stats']['kills']
    else:
        kills = 0
        for i in range(5,10):
            kills += pp['participants'][i]['stats']['kills']

            
    return(100*val/kills)


def fb(pp,pId,teamId):
    stat = pp['participants'][i]['stats']
    
    return(stat['firstBloodKill'] + stat['firstBloodAssist'])


def stats_per_champion(name,date_in,date_out,api,champ):
    
    m = mh_per_week(name,date_in,date_out,api)
    
    cps = most_played_champs_per_week(name,date_in,date_out,apikey1,champ)
    df_work = pd.DataFrame(m[0])    
    keys_chp = df_work['champion'].unique()
    
    key_dict = {}
    
    for i in keys_chp:
        key_dict[i] = {'name': champ[champ['key'] == i]['name'].values[0],
                      'gameids':df_work[df_work['champion'] == i]['gameId'].values}
    
    stats_avg = {}
    for i in keys_chp:
        gameids = key_dict[i]['gameids']
        stats_kp,stats_fb,stats_pink,stats_wp,stats_wk,stats_vs = [],[],[],[],[],[]

        for j in gameids:
            pp = match_information(j,api).json()
            try:
                pId,teamId = find_participant_id(pp,m[1])
                test = True
            except:
                test = None

            if test == None:
                return('ya Erreur')
            
            else:
                stat = pp['participants'][pId-1]['stats']
                
                stats_kp.append(kp(pp,pId,teamId))
                stats_fb.append(fb(pp,pId,teamId))
                stats_pink.append(stat['visionWardsBoughtInGame'])
                stats_wp.append(stat['wardsPlaced']/int(pp['gameDuration']/60))
                stats_wk.append(stat['wardsKilled']/int(pp['gameDuration']/60))
                stats_vs.append(stat['visionScore']/int(pp['gameDuration']/60))
        
        kpp,fbb,pink,wp,wk,vs = mean(stats_kp),mean(stats_fb),mean(stats_pink),mean(stats_wp),mean(stats_wk),mean(stats_vs)
        
        stats_i = {}
        stats_i['champion'] = key_dict[i]['name']

        stats_i['killParticipation'],stats_i['FirstBloodParticipation'],stats_i['visionWardsBoughtInGame'],stats_i['wardsPlacedPerMinute'],stats_i['wardsKilledPerMinute'],stats_i['visionScorePerMinute'] = kpp,fbb,pink,wp,wk,vs
        stats_i['GamesPlayed'] = len(gameids)
        stats_avg[i] = stats_i

    
    
    return(stats_avg)


                    