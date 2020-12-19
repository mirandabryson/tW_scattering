import os
import pandas as pd

deepAK8_SF = os.path.expandvars("$TWHOME/data/deepAK8/DeepAK8V2_Top_W_SFs.csv")

W_SF = pd.read_csv(deepAK8_SF, sep=',')

SF_2016 = W_SF[((W_SF['Object']=='W') & (W_SF['version']=='Nominal') & (W_SF['MistaggingRate']=='1p0') & (W_SF['Year']==2016))][['pT_low', 'pT_high', 'SF']]
SF_2017 = W_SF[((W_SF['Object']=='W') & (W_SF['version']=='Nominal') & (W_SF['MistaggingRate']=='1p0') & (W_SF['Year']==2017))][['pT_low', 'pT_high', 'SF']]
SF_2018 = W_SF[((W_SF['Object']=='W') & (W_SF['version']=='Nominal') & (W_SF['MistaggingRate']=='1p0') & (W_SF['Year']==2018))][['pT_low', 'pT_high', 'SF']]

def getWTagSF(WTag, GenW, year=2016):
    # this is probably the worst lookup in history
    matched_WTag = WTag[WTag.match(GenW, deltaRCut=0.8)]
    
    if year == 2016:
        SF = SF_2016
    elif year == 2017:
        SF = SF_2017
    elif year == 2018:
        SF = SF_2018
    
    sf = (matched_WTag.pt <= 300) * float(SF[['SF']].values[0]) +\
         ((matched_WTag.pt > 300)&(matched_WTag.pt <= 400)) * float(SF[['SF']].values[1]) +\
         (matched_WTag.pt > 400) * float(SF[['SF']].values[2])
    
    return sf.prod()

