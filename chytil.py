import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

month_ele = r"U:\OneDrive_2025-05-10\Dokumenty\Chytil\source\elektrina_mesic"
month_plyn = r"U:\OneDrive_2025-05-10\Dokumenty\Chytil\source\plyn_mesic"
alltime_ele = r"U:\OneDrive_2025-05-10\Dokumenty\Chytil\source\elektrina_alltime"
alltime_plyn = r"U:\OneDrive_2025-05-10\Dokumenty\Chytil\source\plyn_alltime"
ceny_chytil = r"U:\OneDrive_2025-05-10\Dokumenty\Chytil\source\ceny_chytil.xlsx"

final_folder = r"U:\OneDrive_2025-05-10\Dokumenty\Chytil"
prev_month = (datetime.now() - relativedelta(months=1)).strftime("%Y-%m")
final_filename = f"Chytil_Final{prev_month}.xlsx"
final_path = os.path.join(final_folder, final_filename)

# create a function to read the newest file in the folder

def load_newest_excel(folder, header_row=0):
    # listing all the files in the folder, folder is an argument in here
    files = [f for f in os.listdir(folder) if f.endswith(('.xlsx', '.xls'))]
    
    # if the folder is empty, stop the whole operation
    if not files: 
        print(f"No excel files in {folder}")
        return None
    
    # find the newest file by modification time (os.path.join(folder,f) is just creating the path towards the file itself)
    newest = max(files, key= lambda f: os.path.getmtime(os.path.join(folder, f)))
    
    full_path = os.path.join(folder, newest)
    print(f"Loading: {newest}")
    df = pd.read_excel(full_path, header=header_row)
    df = df[[col for col in df.columns if 'Unnamed' not in str(col)]]
    return df

# loading the newest file from every folder using  the load_newest_excel function

df_month_ele = load_newest_excel(month_ele, header_row=1)
df_month_plyn = load_newest_excel(month_plyn, header_row=2)
df_alltime_ele = load_newest_excel(alltime_ele, header_row=1)
df_alltime_plyn = load_newest_excel(alltime_plyn, header_row=2)
df_ceny_chytil = pd.read_excel(ceny_chytil, header=0)

df_month_ele = df_month_ele[[
    'Název zákazníka',
    'Číslo dokladu', 
    'Datum vystavení',
    'Zdanitelné plnění',
    'Číslo\nsmlouvy',
    'EAN odběrného místa', 
    'Obor', 
    'Segment / Kategorie SÚ',
    'Zúčtování od', 
    'Zúčtování do', 
    'Produkt', 
    'Celková spotřeba v MWh']]

df_alltime_ele = df_alltime_ele[[
    'Název zákazníka',
    'Číslo dokladu', 
    'Datum vystavení',
    'Zdanitelné plnění',
    'Číslo\nsmlouvy',
    'EAN odběrného místa', 
    'Obor', 
    'Segment / Kategorie SÚ',
    'Zúčtování od', 
    'Zúčtování do', 
    'Produkt', 
    'Celková spotřeba v MWh']]

df_month_plyn = df_month_plyn[[
    'Název zákazníka',
    'Číslo dokladu',
    'Datum vystavení',
    'Zdanitelné plnění', 
    'Číslo smlouvy', 
    'EIC odběrného místa',
    'Obor',
    'Segment / Kategorie SÚ', 
    'Zúčtování od', 
    'Zúčtování do',
    'Produkt', 
    'Spotřeba MWh']]

df_alltime_plyn = df_alltime_plyn[[
    'Název zákazníka',
    'Číslo dokladu',
    'Datum vyrovnání', 
    'Datum vystavení',
    'Zdanitelné plnění', 
    'Číslo smlouvy', 
    'EIC odběrného místa',
    'Obor',
    'Segment / Kategorie SÚ', 
    'Zúčtování od', 
    'Zúčtování do',
    'Produkt', 
    'Spotřeba MWh']]

# přejmenování sloupců, aby matchovali mezi sebou
df_month_ele = df_month_ele.rename(columns={
    'Číslo\nsmlouvy': 'Číslo smlouvy', 
    'EAN odběrného místa': 'EAN/EIC',
    'Celková spotřeba v MWh': 'Spotřeba MWh'
})
df_month_ele['Datum vystavení'] = pd.to_datetime(df_month_ele['Datum vystavení'])

df_alltime_ele = df_alltime_ele.rename(columns={
    'Číslo\nsmlouvy': 'Číslo smlouvy', 
    'EAN odběrného místa': 'EAN/EIC',
    'Celková spotřeba v MWh': 'Spotřeba MWh'
})

df_month_plyn = df_month_plyn.rename(columns={
   'EIC odběrného místa': 'EAN/EIC' 
})

df_alltime_plyn = df_alltime_plyn.rename(columns={
   'EIC odběrného místa': 'EAN/EIC', 
})
df_month_ele['EAN/EIC']= df_month_ele['EAN/EIC'].astype(str)
df_alltime_ele['Číslo smlouvy'] = df_alltime_ele['Číslo smlouvy'].astype(str)

# filtrace řádků tak, aby se daly použít jen ty, kde jsou reálné faktury
df_alltime_plyn = df_alltime_plyn[(df_alltime_plyn['Název zákazníka'].isnull()) & (df_alltime_plyn['Číslo dokladu'].notna())]
df_alltime_ele = df_alltime_ele[(df_alltime_ele['Název zákazníka'].isnull()) & (df_alltime_ele['Číslo dokladu'].notna())]
df_month_ele = df_month_ele[(df_month_ele['Název zákazníka'].isnull()) & (df_month_ele['Číslo dokladu'].notna())]
df_month_plyn = df_month_plyn[(df_month_plyn['Název zákazníka'].isnull()) & (df_month_plyn['Číslo dokladu'].notna())]
      
# spojeni tabulek 
df_pocitabulka = pd.concat([df_month_ele, df_month_plyn])
df_alltimetabulka= pd.concat([df_alltime_ele, df_alltime_plyn])

# převod datových sloupců do správného formátu a následný datedif
df_pocitabulka['Zúčtování od'] = pd.to_datetime(df_pocitabulka['Zúčtování od'])
df_pocitabulka['Zúčtování do'] = pd.to_datetime(df_pocitabulka['Zúčtování do'])
df_pocitabulka['Datum vystavení'] = pd.to_datetime(df_pocitabulka['Datum vystavení'])
df_pocitabulka['Spotřeba MWh'] = df_pocitabulka['Spotřeba MWh'].astype(float)
df_ceny_chytil['období'] = pd.to_datetime(df_ceny_chytil['období'])
df_pocitabulka['datedif'] = (df_pocitabulka['Zúčtování do'] - df_pocitabulka['Zúčtování od']).dt.days / 30.44
df_alltimetabulka['Datum vystavení'] = pd.to_datetime(df_alltimetabulka['Datum vystavení'])



# dropping duplicates in all time tabulka tak, aby tam byla jen první faktura
df_alltimetabulka = df_alltimetabulka.sort_values('Datum vystavení')
df_alltimetabulka = df_alltimetabulka.drop_duplicates(subset=['Číslo smlouvy'], keep='first')
df_alltimetabulka = df_alltimetabulka.rename(columns={
    'Datum vystavení': 'Datum vystavení první faktury'
})

# Zaokrouhlení datedif sloupce a následné vytvoření řádků podle datedif sloupce 
df_pocitabulka = df_pocitabulka.reset_index(drop=True)
df_pocitabulka['datedif'] = df_pocitabulka['datedif'].round().astype(int)
df_pocitabulka = df_pocitabulka.loc[df_pocitabulka.index.repeat(df_pocitabulka['datedif'])].reset_index(drop=True)

df_pocitabulka['pocitadlo_mesicu'] = df_pocitabulka.groupby('Číslo dokladu').cumcount()
df_pocitabulka['měsíc_fakturace'] = df_pocitabulka.apply(
    lambda row: (row['Zúčtování od'].replace(day=1) + relativedelta(months=row['pocitadlo_mesicu'])), axis=1
)
# napojení ceny z ceny_chytil
df_pocitabulka = df_pocitabulka.merge(
    df_ceny_chytil[['období', 'Attribute', 'Value']], 
    left_on=['měsíc_fakturace', 'Obor'],
    right_on=['období', 'Attribute'],
    how='left'
)
# přejmenování sloupce s cenou
df_pocitabulka = df_pocitabulka.rename(columns={
    'Value': 'Cena v měsíci fakturace'})

# napojení tabulky s prvními fakturami
df_pocitabulka = df_pocitabulka.merge(
    df_alltimetabulka[['Číslo smlouvy', 'Datum vystavení první faktury']], 
    left_on=['Číslo smlouvy'], 
    right_on=['Číslo smlouvy'], 
    how= 'left'
)

df_pocitabulka['Poměr MWh na měsíc fakturace'] = df_pocitabulka['Spotřeba MWh']/df_pocitabulka['datedif']
df_pocitabulka['Finální provize'] = df_pocitabulka['Poměr MWh na měsíc fakturace']*df_pocitabulka['Cena v měsíci fakturace']

df_pocitabulka.to_excel(final_path, index=False)