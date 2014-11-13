# -*- coding: utf-8 -*-
"""
Created on Mon Nov 10 22:13:04 2014

@author: mivinson
"""

#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      MIvinson
#
# Created:     18/09/2014
# Copyright:   (c) MIvinson 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

class sfdc_Select():
    selectDict = {}
    def __init__(self,selectDict):
        self.selectDict = selectDict

    def buildSelect(self):
        select_fields = ''
        sObjects = ''
        whereConditions = ''
        select = ''

        first_record = True
        for fields in self.selectDict['Select']:
            if not first_record: select_fields = select_fields + '\n,'
            else: first_record = False
            thisRecord = fields['Name']
            #Salesforce api does not allow for field aliasing in non-aggregating queries.
##            if fields.has_key('As'):
##                thisRecord = thisRecord + ' As ' + fields['As']
            select_fields = select_fields + thisRecord

        first_record = True
        for objects in self.selectDict['From']:
            if not first_record: sObjects = sObjects + '\n,'
            else: first_record = False
            sObjects = sObjects + objects

        first_record = True
        thisRecord = ''
        for conditions in self.selectDict['Where']:
            if conditions == 'OR':
                if not first_record:
                    whereConditions = whereConditions+' AND('
                    first_record = True
                else: whereConditions = whereConditions + '('
            for statement in self.selectDict['Where'][conditions]:
                if not first_record: thisRecord = '\n%s '%(conditions)
                else: first_record = False
                if statement.has_key('Field') and statement.has_key('Operator') and statement.has_key('Condition'):
                    whereConditions = whereConditions +thisRecord+ '%s %s %s'%(statement['Field'],statement['Operator'],statement['Condition'])
                else:
                    print 'Statement in %s clause is incomplete and being skipped'%(conditions)
                    print statement
            if conditions == 'OR':whereConditions = whereConditions +')'

        print len(whereConditions)
        where = ''
        if len(whereConditions) > 0:
            where = '\nWHERE\n'+whereConditions
        select = 'SELECT\n%s\nFROM\n%s%s'%(select_fields,sObjects,where)
        return select

    def flattenDict(self,d):
        import collections
        result = collections.OrderedDict()
        for k, v in d.items():
            if isinstance(v, dict):
                result.update(flatten(v))
            else:
                result[k] = v
        return result
def main():
    from simple_salesforce import Salesforce
    from sfdcConnect import sfdc_Select
    import datetime
    import pytz
    import json
    import sqlite3
    import numpy as np
    import io
    from secret import *


    username = USER
    password = PASSWORD
    security_token = HKEY

    ##req = sf.query_all("SELECT Name, Industry FROM Account WHERE (CreatedDate >= 2014-09-19T00:00:00Z and  CreatedDate < 2014-09-20T00:00:00Z) or (LastModifiedDate >= 2014-09-19T00:00:00Z and LastModifiedDat < 2014-09-20T00:00:00Z) LIMIT 125")
    ##response = req.json()

    today = datetime.date.today()

    account_fields = [{'Name':'Id','Type':'TEXT','Key':'PRIMARY KEY'},
    {'Name':'IsDeleted','Type':'TEXT'},
    {'Name':'MasterRecordId','Type':'TEXT'},
    {'Name':'Name','Type':'TEXT'},
    {'Name':'Type','Type':'TEXT'},
    {'Name':'ParentId','Type':'TEXT'},
    {'Name':'BillingStreet','Type':'TEXT'},
    {'Name':'BillingCity','Type':'TEXT'},
    {'Name':'BillingState','Type':'TEXT'},
    {'Name':'BillingPostalCode','Type':'TEXT'},
    {'Name':'BillingCountry','Type':'TEXT'},
    {'Name':'BillingLatitude','Type':'TEXT'},
    {'Name':'BillingLongitude','Type':'TEXT'},
    {'Name':'ShippingStreet','Type':'TEXT'},
    {'Name':'ShippingCity','Type':'TEXT'},
    {'Name':'ShippingState','Type':'TEXT'},
    {'Name':'ShippingPostalCode','Type':'TEXT'},
    {'Name':'ShippingCountry','Type':'TEXT'},
    {'Name':'ShippingLatitude','Type':'TEXT'},
    {'Name':'ShippingLongitude','Type':'TEXT'},
    ##{'Name':'Phone'},
    ##{'Name':'Fax'},
    {'Name':'AccountNumber','Type':'TEXT'},
    ##{'Name':'Website'},
    ##{'Name':'Sic'},
    {'Name':'Industry','Type':'TEXT'},
    {'Name':'AnnualRevenue','Type':'REAL'},
    {'Name':'NumberOfEmployees','Type':'INTEGER'},
    {'Name':'Ownership','Type':'TEXT'},
    {'Name':'TickerSymbol','Type':'TEXT'},
    {'Name':'Description','Type':'TEXT'},
    {'Name':'Site','Type':'TEXT'},
    {'Name':'CurrencyIsoCode','Type':'TEXT'},
    {'Name':'OwnerId','Type':'TEXT'},
    {'Name':'CreatedDate','Type':'TEXT'},
    {'Name':'CreatedById','Type':'TEXT'},
    {'Name':'LastModifiedDate','Type':'TEXT'},
    {'Name':'LastModifiedById','Type':'TEXT'},
    {'Name':'SystemModstamp','Type':'TEXT'},
    ##{'Name':'LastActivityDate'},
    ##{'Name':'LastViewedDate'},
    ##{'Name':'LastReferencedDate'},
    ##{'Name':'Jigsaw'},
    ##{'Name':'JigsawCompanyId'},
    ##{'Name':'CleanStatus'},
    ##{'Name':'AccountSource'},
    {'Name':'DunsNumber','Type':'TEXT'},
    ##{'Name':'Tradestyle'},
    {'Name':'NaicsCode','Type':'TEXT'},
    {'Name':'NaicsDesc','Type':'TEXT'},
    ##{'Name':'YearStarted'},
    ##{'Name':'SicDesc'},
    ##{'Name':'WHCDB_Days_since_last_update_or_activity__c'},
    {'Name':'Sales_Team_Assigned__c', 'As':'AssignedTeam','Type':'TEXT'},
    ##{'Name':'Rank__c'},
    ##{'Name':'Data_Quality_Description__c'},
    ##{'Name':'Data_Quality_Score__c'},
    ##{'Name':'Requested_By__c'},
    ##{'Name':'Discount__c'},
    ##{'Name':'Total_Value_of_Closed__c'},
    ##{'Name':'Total_Value_of_Lost_Opportunities__c'},
    ##{'Name':'No_of_Won_Opportunities__c'},
    ##{'Name':'No_of_Lost_Opportunities__c'},
    ##{'Name':'Status__c'},
    {'Name':'VAT_Number__c', 'As':'VATNumber','Type':'TEXT'},
    {'Name':'Primary_US_SIC_Code__c', 'As':'PrimarySIC','Type':'TEXT'},
    {'Name':'Doing_Business_As__c', 'As':'DBAName','Type':'TEXT'},
    ##{'Name':'Ownership_Year__c'},
    ##{'Name':'Fiscal_Year_End__c'},
    {'Name':'Latest_Annual_Revenue__c', 'As':'LatestAnnualRevenue','Type':'TEXT'},
    ##{'Name':'Company_Registration_Number__c'},
    ##{'Name':'Immediate_Parent_Name__c'},
    ##{'Name':'Immediate_Parent_DUNS_Number__c'},
    ##{'Name':'Ultimate_Parent_Name__c'},
    ##{'Name':'Ultimate_Parent_D_U_N_S_Number__c'},
    ##{'Name':'Company_Type__c'},
    ##{'Name':'Location_Type__c'},
    ##{'Name':'Synopsis__c'},
    ##{'Name':'Overview__c'},
    ##{'Name':'Last_Years__c'},
    ##{'Name':'Latest_Information_Year__c'},
    {'Name':'Revenue__c', 'As':'Revenue','Type':'REAL'},
    ##{'Name':'Sales_Growth__c'},
    {'Name':'Net_Income__c', 'As':'NetIncome','Type':'REAL'},
    {'Name':'Net_Income_Growth__c', 'As':'NetIncomeGrowth','Type':'REAL'},
    {'Name':'Employees_at_this_Location__c', 'As':'EmployeeCount','Type':'REAL'},
    ##{'Name':'Assets__c'},
    ##{'Name':'Market_Value__c'},
    ##{'Name':'State_of_Incorporation__c'},
    {'Name':'US_Ticker_Symbol__c', 'As':'USTicker','Type':'TEXT'},
    {'Name':'CSR_Date__c', 'As':'CSRDate','Type':'TEXT'},
    ##{'Name':'Primary_NAICS_Code__c'},
    ##{'Name':'Primary_UK_SIC_Code__c'},
    ##{'Name':'All_US_SIC_Codes__c'},
    ##{'Name':'All_NAICS_Codes__c'},
    ##{'Name':'All_UK_SIC_Codes__c'},
    {'Name':'Region__c', 'As':'Region','Type':'TEXT'},
    {'Name':'Oracle_AccountID__c', 'As':'OracleAccountID','Type':'TEXT'},
    {'Name':'SFDC_Account_ID__c', 'As':'SFDCAccountID','Type':'TEXT'},
    {'Name':'Address1_Line_1__c', 'As':'Address1Line1','Type':'TEXT'},
    {'Name':'Address1_Line_2__c', 'As':'Address1Line2','Type':'TEXT'},
    {'Name':'Address1_Line_3__c', 'As':'Address1Line3','Type':'TEXT'},
    {'Name':'City1__c', 'As':'Address1City','Type':'TEXT'},
    {'Name':'State1__c', 'As':'Address1State','Type':'TEXT'},
    {'Name':'Zip_Code1__c', 'As':'Address1ZipCode','Type':'TEXT'},
    {'Name':'Country1__c', 'As':'Address1Country','Type':'TEXT'},
    {'Name':'Address2_Line_1__c', 'As':'Address2Line1','Type':'TEXT'},
    {'Name':'Address2_Line_2__c', 'As':'Address2Line2','Type':'TEXT'},
    {'Name':'Address2_Line_3__c', 'As':'Address2Line3','Type':'TEXT'},
    {'Name':'City2__c', 'As':'Address2City','Type':'TEXT'},
    {'Name':'Country2__c', 'As':'Address2Country','Type':'TEXT'},
    {'Name':'State2__c', 'As':'Address2State','Type':'TEXT'},
    {'Name':'Zip_Code2__c', 'As':'Address2ZipCode','Type':'TEXT'},
    {'Name':'Office__c', 'As':'Office','Type':'TEXT'},
    {'Name':'HooversId__c', 'As':'HooversID','Type':'TEXT'},
    ##{'Name':'Is_New_Account__c'},
    {'Name':'D_U_N_S__c', 'As':'DUNS','Type':'TEXT'},
    ##{'Name':'Oracle_API_Status__c'},
    ##{'Name':'OwnerNameSync__c'},
    ##{'Name':'Vendor__c'},
    ##{'Name':'CSR__c'},
    {'Name':'Total_Invoice_Amount__c', 'As':'TotalInvoiced','Type':'TEXT'},
    {'Name':'Terms_Of_Sale__c', 'As':'SalesTerms','Type':'TEXT'},
    {'Name':'Sales_Channel__c', 'As':'SalesChannel','Type':'TEXT'},
    ##{'Name':'BDD_Date__c'},
    ##{'Name':'Active__c'},
    ##{'Name':'Hold_Reasons__c'},
    ##{'Name':'On_Hold__c'},
    {'Name':'Oracle_PartyID__c', 'As':'OraclePartyID','Type':'TEXT'},
    ##{'Name':'Vendor_Type__c'},
    ##{'Name':'Oracle_VendorID__c'},
    ##{'Name':'When_Last_Opportunity_Won__c'},
    ##{'Name':'Days_Since_Last_Opportunity_Won__c'},
    ##{'Name':'PaymentMethod__c'},
    ##{'Name':'IntegrationError__c'},
    ##{'Name':'StrikeForce5__Billing_Status_Display__c'},
    ##{'Name':'StrikeForce5__Billing_Status__c'},
    ##{'Name':'StrikeForce5__Billing_Verify__c'},
    ##{'Name':'StrikeForce5__Shipping_Status_Display__c'},
    ##{'Name':'StrikeForce5__Shipping_Status__c'},
    ##{'Name':'StrikeForce5__Shipping_Verify__c'},
    ##{'Name':'CreditExposure__c'},
    ##{'Name':'CreditLimit__c'},
    ##{'Name':'CreditLimit_Currency__c'},
    ##{'Name':'Domestic_Ultimate_D_U_N_S_Number__c'},
    ##{'Name':'Domestic_Parent_Name__c'},
    ##{'Name':'Hierarchy_Code__c'},
    ##{'Name':'Status_Code__c'},
    ##{'Name':'Subsidiary_Code__c'},
    ##{'Name':'HQ_DUNS_Number__c'},
    ##{'Name':'Vendor_Owner__c'},
    ##{'Name':'Data_Migration__c'},
    ##{'Name':'VendorTerms__c'},
    ##{'Name':'Billing_County__c'},
    ##{'Name':'D_And_B__c'},
    ##{'Name':'CEO_Name__c'},
    ##{'Name':'Affiliated__c'},
    ##{'Name':'Warranty_Offered__c'},
    ##{'Name':'PricingSensitivity__c'},
    ##{'Name':'Enriched__c'},
    ##{'Name':'Global_Ultimate_Indicator__c'},
    ##{'Name':'Vendor_Rating__c'},
    ##{'Name':'Team_tag__c'},
    ##{'Name':'OneSource__OSKeyID__c'},
    ##{'Name':'NetSure_Customer_c__c'},
    ##{'Name':'Zendesk__Zendesk_Organization__c'},
    ##{'Name':'My_Team_s_Account__c'},
    ##{'Name':'Reason_Account_K_d__c'},
    {'Name':'K_Date__c', 'As':'Kdate','Type':'TEXT'},
    {'Name':'Prior_Sales_Team__c', 'As':'PriorSalesTeam','Type':'TEXT'},
    {'Name':'K_Information__c', 'As':'Kinformation','Type':'TEXT'},
    {'Name':'Sales_Team_Name__c', 'As':'SalesTeamName','Type':'TEXT'},
    ##{'Name':'Referral__c'},
    ##{'Name':'Country3__c'},
    ##{'Name':'Last_Activity_Date__c'},
    ##{'Name':'Last_Activity__c'},
    ##{'Name':'ST_I2__Notepad__c'},
    ##{'Name':'eBay_Lead__c'},
    {'Name':'Acc18ID__c', 'As':'SFDC18AcctID','Type':'TEXT'},
    ##{'Name':'Mgmt_Date__c'},
    ##{'Name':'Grandfather_Account__c'},
    ##{'Name':'Strategic_Account__c'},
    ##{'Name':'ROI_Lead_Source__c'},
    {'Name':'ORS_Ownership_Date__c', 'As':'ORSOwnershipDate','Type':'TEXT'},
    {'Name':'ORS__c', 'As':'ORS','Type':'TEXT'},
    ##{'Name':'rk_Lead_Source__c'},
    ##{'Name':'Subsidiary__c'},
    ##{'Name':'rkpi2__rkCompanyId__c'},
    ##{'Name':'rkpi2__rk_DefaultVisibility__c'},
    ##{'Name':'rkpi2__rk_RetrievalFlag__c'},
    {'Name':'Fortune_500__c', 'As':'Fortune500','Type':'TEXT'}
    ##{'Name':'Account_Classification__c'},
    ##{'Name':'Account_Class_MGR__c'},
    ##{'Name':'Split_Percent__c'},
    ##{'Name':'Second_Team_Lookup__c'},
    ##{'Name':'MGMT_Date_Strat__c'},
    ##{'Name':'MDC_Team__c'},
    ##{'Name':'Auto_Invoice_Email_1__c'},
    ##{'Name':'Auto_Invoice_Email_2__c'},
    ##{'Name':'Auto_Invoice_Email_3__c'},
    ##{'Name':'Auto_Invoice_Email_4__c'},
    ##{'Name':'Auto_Invoice_Email_5__c'},
    ##{'Name':'Auto_Invoice_Email_6__c'},
    ]

    from_object = ['Account']
    n = 2
##    where_Clause = {}
    where_Clause = {'AND':[{'Field':'CreatedDate','Operator':'=','Condition':'LAST_N_QUARTERS:%s'%(str(n))},
                          {'Field':'LastModifiedDate','Operator':'!=','Condition':'LAST_N_QUARTERS:%s'%(str(n-1))}]}
##                    ,'OR':[]}

    selectDict = {'Select':account_fields,'From':from_object,'Where':where_Clause}
    output = []

    select = sfdc_Select(selectDict)
##    print select.buildSelect()
    sf = Salesforce(username = username, password = password,security_token=security_token)
#    req = sf.query_all(select.buildSelect()+' LIMIT 10')
    print select.buildSelect()
#    createHeaders = ''
#    columnNames = ''
#    valueSpace = ''
#    firstRecord = True
#    for records in account_fields:
#        if not firstRecord:
#            createHeaders = createHeaders + ',\n'
#            columnNames = columnNames + ', '
#            valueSpace = valueSpace +', '
#        else: firstRecord = False
#        if records.has_key('Key'): key = ' ' +records['Key']
#        else: key = ''
#        if records.has_key('As'):
#            createHeaders = createHeaders + records['As'] +' '+ records['Type'] + key
#            columnNames = columnNames + records['As']
#        else:
#            createHeaders = createHeaders + records['Name'] +' '+ records['Type'] + key
#            columnNames = columnNames + records['Name']
#        valueSpace = valueSpace + '?'
#    print createHeaders
#    print columnNames
#
#    for results in req['records']:
#        thisRecord = []
#        for records in account_fields:
#            thisRecord.append(results[records['Name']])
#        output.append(thisRecord)
#    #print output
#
#    print len(account_fields)
##    print output
##    array = np.array(output)
##    print array

##    def adapt_array(arr):
##        out = io.BytesIO()
##        np.save(out, arr)
##        out.seek(0)
##        # http://stackoverflow.com/a/3425465/190597 (R. Hill)
##        return buffer(out.read())
##
##
##    def convert_array(text):
##        out = io.BytesIO(text)
##        out.seek(0)
##        return np.load(out)
##
##    # Converts np.array to TEXT when inserting
##    sqlite3.register_adapter(np.ndarray, adapt_array)
##
##    # Converts TEXT to np.array when selecting
##    sqlite3.register_converter("array", convert_array)

##    print array
##    x = array

#    con = sqlite3.connect('P:\Accounting\Programming Projects\Salesforce Select\example.db')
#    cur = con.cursor()
#
#    tableName = 'SFDC_Accounts'
#    tempTable = 'temp_sfdc_accounts'
#
#    try:
#        cur.execute("create table %s (%s)"%(tableName,createHeaders))
#        print tableName + ' TABLE CREATED!'
#    except: print tableName + 'TABLE ALREADY EXISTS!'
#
#    try:
#        cur.execute("create table %s (%s)"%(tempTable,createHeaders))
#    except: print 'temp_sfdc_accounts already exists'
#
#
#    cur.execute("insert into %s (%s) values (%s)"%(tempTable,columnNames,valueSpace),output)
###    cur.execute("insert into %s SELECT * FROM temp_sfdc_accounts"%(tableName))

##
#    cur.execute("select * from %s"%(tempTable))
###    cur.execute("PRAGMA table_info(test);")
#    data = cur.fetchone()[0]
#
#    print data



##    flattenedreq = select.flattenDict(req)
##    print flattenedreq

if __name__ == '__main__':
    main()

