import csv
rows = list(csv.DictReader(open('CustomersProjects412.csv', encoding='utf-8-sig')))
targets = ['994','1029','1009','667','1094','1037','1047','1048','1040','1265','1436','1562','1611','1967','1968','2019','2033','2217','2318','2849','3000','1002','3551','3589','3651','1230','3666','3670','3710','3711']
for r in rows:
    if r['Internal ID'] in targets:
        print(r['Internal ID'] + " | " + r['ID'] + " | " + r['Company Name'])