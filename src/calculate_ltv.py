import json,heapq
from datetime import datetime

def Ingest(e, D):
    #Creating Data structure from event
    
    if e["type"] == "CUSTOMER":
        common_key = e["key"]
    else:
        common_key = e["customer_id"]
    
    # Event time max and min date time calculating
    event_time = datetime.strptime(e["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
    D["Max_Date"] = max(event_time,D["Max_Date"])
    D["Min_Date"] = min(event_time,D["Min_Date"])
    print(D)

    # Since customer ID in common in all events so creating the data structure with customer id 
    if common_key not in D["data"].keys():
        D["data"][common_key] = { "CUSTOMER" :{"Last_Name":"" , "Adress_City": "","Adress_State":"","event_time":datetime.min,"joinDate": datetime.max},
                            "SITE_VISIT":0 ,
                            "SITE_VISIT_DETAIL" : {},
                            "ORDER":{"total_amount":0,"order_count":0},
                            "ORDER_DETAIL" : {}
                             }
    
    if e["type"] == "CUSTOMER":         
            # Record is not update when record time is less than join date
            if event_time > D["data"][common_key]["CUSTOMER"]["event_time"]:
                D["data"][common_key]["CUSTOMER"]["last_name"] = e["last_name"]
                D["data"][common_key]["CUSTOMER"]["adr_city"] = e["adr_city"]
                D["data"][common_key]["CUSTOMER"]["adr_state"] = e["adr_state"]
                D["data"][common_key]["CUSTOMER"]["event_time"] = event_time
                D["data"][common_key]["CUSTOMER"]["joinDate"] = min(D["data"][common_key]["CUSTOMER"]["joinDate"],event_time)
            else:
                D["data"][common_key]["CUSTOMER"]["joinDate"] = min(D["data"][common_key]["CUSTOMER"]["joinDate"],event_time)
            
    elif e["type"] == "SITE_VISIT":     
        #add and update the number of visit
        D["data"][common_key]["SITE_VISIT"] += 1
        D["data"][common_key]["SITE_VISIT_DETAIL"]["key"] = e["key"]
        D["data"][common_key]["SITE_VISIT_DETAIL"]["event_time"] = event_time
        D["data"][common_key]["SITE_VISIT_DETAIL"]["tags"] = e["tags"]
        
   
    elif e["type"] == "ORDER":  
        
        #take only the numeric value in the total_amount 
        try:
            total_amount = float(e["total_amount"].split(" ")[0])
        except:
            print("Order total amount not valid :" +e)
            return 
        
        if e["key"] not in D["data"][common_key]["ORDER_DETAIL"].keys():
            #If new order it will save with event time and amount
            D["data"][common_key]["ORDER_DETAIL"][ e["key"]] = (total_amount,event_time)
            D["data"][common_key]["ORDER"]["order_count"] += 1
            D["data"][common_key]["ORDER"]["total_amount"] += total_amount
        elif event_time > D["data"][common_key]["ORDER_DETAIL"][ e["key"]][1] :
                #if the event date is greater than saved record ORDER total_amount is updated 
                D["data"][common_key]["ORDER"]["total_amount"] =  D["data"][common_key]["ORDER"]["total_amount"] \
                                                                    + (total_amount - D["data"][common_key]["ORDER_DETAIL"][ e["key"]][0])
                    
                #Order detail record is updated based on the event key of the ORDER event
                D["data"][common_key]["ORDER_DETAIL"][ e["key"]] = (total_amount,event_time)
  

def TopXSimpleLTVCustomers(x, D):
 
    Top_LTV_by_Value=[]
    ComputedValue = {} # Key = customer id with total amount
    xRunner = 0
    t = 10
    
    LastdataDate = D["Max_Date"]
    MindataDate = D["Min_Date"]
    CustList = D["data"].keys()
    
    for customer_id in CustList:

        if  D["data"][customer_id]["ORDER"]["order_count"] > 0 and \
            D["data"][customer_id]["ORDER"]["total_amount"] > 0 and \
                D["data"][customer_id]["CUSTOMER"]["joinDate"] >= MindataDate and \
                D["data"][customer_id]["CUSTOMER"]["joinDate"] <= LastdataDate and \
                D["data"][customer_id]["SITE_VISIT"] > 0:
            
            #Total Order Amount
            orderedAmount = D["data"][customer_id]["ORDER"]["total_amount"]
            #Visits Per week

            if (LastdataDate - D["data"][customer_id]["CUSTOMER"]["joinDate"]).days <7:
                weeks = 1
            else :
                weeks = ((LastdataDate - D["data"][customer_id]["CUSTOMER"]["joinDate"]).days /7)
            #calculate  LTV
            value = (52 * (orderedAmount / weeks)) * t
            
            #If the value does not exist then add to the add customerid as a list value
            if value not in ComputedValue.keys():
                ComputedValue[value] = [customer_id]
            else:
                ComputedValue[value].append(customer_id)
            Top_LTV_by_Value.append(value)
                
    sorted_Top_LTV_by_Value = sorted(Top_LTV_by_Value, reverse=True)
    print(sorted_Top_LTV_by_Value)
    
    list=[ComputedValue[k] for k in sorted_Top_LTV_by_Value]
    flattened_list = [item for sublist in list for item in sublist]
    
    xRunner = 0                
    output_file = open(r"C:\Users\Bhargava Reddy\Downloads\challenge\breddy\code-challenge\output\output.txt", "w")

    for customer_id in flattened_list:
        if xRunner < x:
            output_file.write(customer_id+"\n" )
            xRunner += 1
        else:
            gotTopX = True
    output_file.close()  

if __name__ == "__main__":

    D = {"data": {},"Max_Date":datetime.min,"Min_Date":datetime.max}
    input_file_path = r"C:\Users\Bhargava Reddy\Downloads\challenge\breddy\code-challenge\input\input.txt"
    read_file = json.load(open(input_file_path)) 

    #Adding data to the data structure
    for x in read_file:
        
        Ingest(x,D)
    #Computing TopX
    x = 10
    TopXSimpleLTVCustomers(x, D)