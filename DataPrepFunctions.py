
def CleanDuplicatedValue(message):
    message['key'] = message[['ID_CLIENT','ID_CONTRAT','Code_Naf']].astype(str).sum(axis=1)
    return message.drop_duplicates()\
                   .sort_values(by='ID_CLIENT')\
                   .groupby([i for i in message.columns if i != 'NB_EMPLOYEES_CLIENT'])\
                   .sum()\
                   .reset_index()\
                   .groupby(['key','ID_CONTRAT',
                                     'ID_CLIENT',
                                     'DEP',
                                     'Code_Naf',
                                     'NB_EMPLOYEES_CLIENT']).sum()\
                   .reset_index()

def CAP_GROUP(x):
    if x < 0.1:
    	return 'Classic'
    
    else : 
    	return 'Premium'

    
def AddIndicators(messages):
    MessagesBucketVariables = [i for i in messages.columns if 'Messages' in i]

    messages = messages[['ID_CONTRAT',
                         'ID_CLIENT',
                         'DEP',
                         'Code_Naf',
                         'NB_EMPLOYEES_CLIENT']+ MessagesBucketVariables]


    messages_agg_client = (messages.groupby('ID_CLIENT')['DEP'].apply(lambda x:list(set(x)))).to_frame()\
          .join(\
                messages.groupby('ID_CLIENT')['ID_CONTRAT']\
                        .count().to_frame(name='NB_CONTRAT'))\
          .join(\
                messages.groupby('ID_CLIENT')['Code_Naf']\
                         .apply(lambda x:list(set(x))).to_frame(name='Liste_Code_Naf'))\
          .join(\
                messages.groupby('ID_CLIENT')[['NB_EMPLOYEES_CLIENT']+ MessagesBucketVariables]\
                         .sum())

    messages_agg_client['MESSAGE_TOTALE'] = messages_agg_client[MessagesBucketVariables].sum(axis=1)

    messages_agg_client['MESSAGE_RISQUE'] = messages_agg_client[MessagesBucketVariables].std(axis=1)

    messages_agg_client['MESSAGE_MOYEN'] = messages_agg_client[MessagesBucketVariables].mean(axis=1)

    messages_agg_client['MESSAGE_MEDIAN'] = messages_agg_client[MessagesBucketVariables].median(axis=1)


    # on garde uniquement les clients qui ont au moins 10 messages 

    messages_agg_client = messages_agg_client.query('MESSAGE_TOTALE >=10')

    messages_agg_client.sort_values(by='MESSAGE_TOTALE',ascending=False,inplace=True)

    messages_agg_client['RANK'] = np.arange(len(messages_agg_client))+1

    total_message = messages_agg_client['MESSAGE_TOTALE'].sum()

    messages_agg_client = messages_agg_client\
                         .assign(PERCENT_RANK = lambda x:1-((x.RANK-1)/(len(messages_agg_client)-1)))\
                         .assign(MARKET_CAP =  lambda x:(x.MESSAGE_TOTALE / total_message)*100)\
                         .assign(CUM_MARKET_CAP = lambda x:(x.MESSAGE_TOTALE.cumsum()/total_message)*100)

    messages_agg_client['ACTIVITY'] = messages_agg_client.apply(lambda x: (x[MessagesBucketVariables]>0).sum(),axis=1)

    messages_agg_client['ACTIVITY_RATE'] = (messages_agg_client['ACTIVITY'] /len(MessagesBucketVariables))*100

    # Formule de sharpe ratio : détermine la rentabilité d'un portefeuille
    # https://fr.wikipedia.org/wiki/Ratio_de_Sharpe
    # En posant comme r (référentiel de comparaison) message moyen par mois sur tout l'historique  
    messages_agg_client['RENTABILITY_SCORE'] = (messages_agg_client['MESSAGE_MOYEN'] -  messages_agg_client['MESSAGE_MOYEN'].mean()) / messages_agg_client['MESSAGE_RISQUE']

    messages_agg_client.insert(3, 'NB_SECTEURS',messages_agg_client['Liste_Code_Naf'].apply(lambda x : len(x)))
    
    messages_agg_client.insert(2, 'Code_Naf',messages_agg_client['Liste_Code_Naf'].apply(lambda x : x[0]))

    messages_agg_client['DEP'] = messages_agg_client['DEP'].apply(lambda x:list(x)[0])
    
    messages_agg_client['GROUP_CAP'] = messages_agg_client['MARKET_CAP'].apply(lambda x: CAP_GROUP(x))

    messages_agg_client.reset_index(inplace=True)
    
    departement_detail.drop_duplicates(subset='Code Département',keep='first',inplace=True)
    
    code_naf.drop_duplicates(subset='Code_Naf',keep='first',inplace=True)
    
    messages_agg_client = messages_agg_client.merge(departement_detail,
                                                     left_on='DEP',
                                                     right_on = 'Code Département',
                                                     how='left'
                                                     )\
                                              .merge(code_naf,
                                                     left_on='Code_Naf',
                                                     right_on = 'Code_Naf',
                                                     how='left'
                                                     )

    return messages, messages_agg_client
