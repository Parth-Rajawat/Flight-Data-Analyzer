import random
import json
from datetime import datetime, timedelta, timezone

# ----------------------------------------
# Airports & Airlines (add more if needed)
# ----------------------------------------

airports = [
    ("Hartsfield–Jackson Atlanta International Airport", "ATL", "KATL", "America/New_York"),
    ("Beijing Capital International Airport", "PEK", "ZBAA", "Asia/Shanghai"),
    ("Los Angeles International Airport", "LAX", "KLAX", "America/Los_Angeles"),
    ("Dubai International Airport", "DXB", "OMDB", "Asia/Dubai"),
    ("Tokyo Haneda Airport", "HND", "RJTT", "Asia/Tokyo"),
    ("O'Hare International Airport", "ORD", "KORD", "America/Chicago"),
    ("London Heathrow Airport", "LHR", "EGLL", "Europe/London"),
    ("Hong Kong International Airport", "HKG", "VHHH", "Asia/Hong_Kong"),
    ("Shanghai Pudong International Airport", "PVG", "ZSPD", "Asia/Shanghai"),
    ("Paris Charles de Gaulle Airport", "CDG", "LFPG", "Europe/Paris"),
    ("Amsterdam Schiphol Airport", "AMS", "EHAM", "Europe/Amsterdam"),
    ("Frankfurt am Main Airport", "FRA", "EDDF", "Europe/Berlin"),
    ("Istanbul Airport", "IST", "LTFM", "Europe/Istanbul"),
    ("Singapore Changi Airport", "SIN", "WSSS", "Asia/Singapore"),
    ("Incheon International Airport", "ICN", "RKSI", "Asia/Seoul"),
    ("Denver International Airport", "DEN", "KDEN", "America/Denver"),
    ("Soekarno–Hatta International Airport", "CGK", "WIII", "Asia/Jakarta"),
    ("Indira Gandhi International Airport", "DEL", "VIDP", "Asia/Kolkata"),
    ("Suvarnabhumi Airport", "BKK", "VTBS", "Asia/Bangkok"),
    ("San Francisco International Airport", "SFO", "KSFO", "America/Los_Angeles"),
    ("Seattle–Tacoma International Airport", "SEA", "KSEA", "America/Los_Angeles"),
    ("Miami International Airport", "MIA", "KMIA", "America/New_York"),
    ("Toronto Pearson International Airport", "YYZ", "CYYZ", "America/Toronto"),
    ("Dallas/Fort Worth International Airport", "DFW", "KDFW", "America/Chicago"),
    ("Guangzhou Baiyun International Airport", "CAN", "ZGGG", "Asia/Shanghai"),
    ("Madrid–Barajas Airport", "MAD", "LEMD", "Europe/Madrid"),
    ("Barcelona–El Prat Airport", "BCN", "LEBL", "Europe/Madrid"),
    ("Chhatrapati Shivaji Maharaj International Airport", "BOM", "VABB", "Asia/Kolkata"),
    ("Vienna International Airport", "VIE", "LOWW", "Europe/Vienna"),
    ("Zurich Airport", "ZRH", "LSZH", "Europe/Zurich"),
    ("Munich Airport", "MUC", "EDDM", "Europe/Berlin"),
    ("Brussels Airport", "BRU", "EBBR", "Europe/Brussels"),
    ("Copenhagen Airport", "CPH", "EKCH", "Europe/Copenhagen"),
    ("Oslo Gardermoen Airport", "OSL", "ENGM", "Europe/Oslo"),
    ("Stockholm Arlanda Airport", "ARN", "ESSA", "Europe/Stockholm"),
    ("Helsinki Airport", "HEL", "EFHK", "Europe/Helsinki"),
    ("Doha Hamad International Airport", "DOH", "OTHH", "Asia/Qatar"),
    ("Abu Dhabi International Airport", "AUH", "OMAA", "Asia/Dubai"),
    ("Kuala Lumpur International Airport", "KUL", "WMKK", "Asia/Kuala_Lumpur"),
    ("Perth Airport", "PER", "YPPH", "Australia/Perth"),
    ("Melbourne Airport", "MEL", "YMML", "Australia/Melbourne"),
    ("Sydney Kingsford Smith Airport", "SYD", "YSSY", "Australia/Sydney"),
    ("Auckland Airport", "AKL", "NZAA", "Pacific/Auckland"),
    ("Wellington Airport", "WLG", "NZWN", "Pacific/Auckland"),
    ("Christchurch Airport", "CHC", "NZCH", "Pacific/Auckland"),
    ("Cape Town International Airport", "CPT", "FACT", "Africa/Johannesburg"),
    ("Johannesburg OR Tambo Airport", "JNB", "FAOR", "Africa/Johannesburg"),
    ("Cairo International Airport", "CAI", "HECA", "Africa/Cairo"),
    ("Casablanca Mohammed V International Airport", "CMN", "GMMN", "Africa/Casablanca"),
    ("Lagos Murtala Muhammed Airport", "LOS", "DNMM", "Africa/Lagos"),
    ("Doha International Airport", "DOH", "OTHH", "Asia/Qatar"),
    ("Abu Dhabi International Airport", "AUH", "OMAA", "Asia/Dubai"),
    ("Bangalore Kempegowda International", "BLR", "VOBL", "Asia/Kolkata"),
    ("Hyderabad Rajiv Gandhi Intl", "HYD", "VOHS", "Asia/Kolkata"),
    ("Chengdu Shuangliu International", "CTU", "ZUUU", "Asia/Shanghai"),
    ("Shenzhen Bao'an International", "SZX", "ZGSZ", "Asia/Shanghai"),
    ("Xi'an Xianyang International", "XIY", "ZLXY", "Asia/Shanghai"),
    ("Kolkata Netaji Subhas Chandra Bose", "CCU", "VECC", "Asia/Kolkata"),
    ("Vienna International", "VIE", "LOWW", "Europe/Vienna"),
    ("Prague Václav Havel Airport", "PRG", "LKPR", "Europe/Prague"),
    ("Budapest Ferenc Liszt International", "BUD", "LHBP", "Europe/Budapest"),
    ("Warsaw Chopin Airport", "WAW", "EPWA", "Europe/Warsaw"),
    ("Athens Eleftherios Venizelos", "ATH", "LGAV", "Europe/Athens"),
    ("Lisbon Humberto Delgado Airport", "LIS", "LPPT", "Europe/Lisbon"),
    ("Nice Côte d'Azur Airport", "NCE", "LFMN", "Europe/Paris"),
    ("Malaga Costa Del Sol Airport", "AGP", "LEMG", "Europe/Madrid"),
    ("Moscow Sheremetyevo", "SVO", "UUEE", "Europe/Moscow"),
    ("St. Petersburg Pulkovo", "LED", "ULLI", "Europe/Moscow"),
    ("Istanbul Sabiha Gökçen", "SAW", "LTFJ", "Europe/Istanbul"),
    ("Tehran Imam Khomeini Intl", "IKA", "OIIE", "Asia/Tehran"),
    ("Baghdad International Airport", "BGW", "ORBI", "Asia/Baghdad"),
    ("Jeddah King Abdulaziz Intl", "JED", "OEJN", "Asia/Riyadh"),
    ("Riyadh King Khalid Intl", "RUH", "OERK", "Asia/Riyadh"),
    ("Tel Aviv Ben Gurion Airport", "TLV", "LLBG", "Asia/Jerusalem"),
    ("Doha Al Udeid Air Base", "XJD", "OTBH", "Asia/Qatar"),
    ("Muscat International Airport", "MCT", "OOMS", "Asia/Muscat"),
    ("Amman Queen Alia Intl", "AMM", "OJAI", "Asia/Amman"),
    ("Kuwait International Airport", "KWI", "OKBK", "Asia/Kuwait"),
    ("Bahrain International Airport", "BAH", "OBBI", "Asia/Bahrain"),
    ("Beirut–Rafic Hariri Intl", "BEY", "OLBA", "Asia/Beirut"),
    ("Karachi Jinnah International", "KHI", "OPKC", "Asia/Karachi"),
    ("Lahore Allama Iqbal Intl", "LHE", "OPLA", "Asia/Karachi"),
    ("Islamabad International Airport", "ISB", "OPIS", "Asia/Karachi"),
    ("Tashkent Islam Karimov Intl", "TAS", "UTTT", "Asia/Tashkent"),
    ("Almaty International Airport", "ALA", "UAAA", "Asia/Almaty"),
    ("Baku Heydar Aliyev Intl", "GYD", "UBBB", "Asia/Baku"),
    ("Yerevan Zvartnots International", "EVN", "UDYZ", "Asia/Yerevan"),
    ("Tbilisi International Airport", "TBS", "UGTB", "Asia/Tbilisi"),
    ("Colombo Bandaranaike Intl", "CMB", "VCBI", "Asia/Colombo"),
    ("Male Velana International Airport", "MLE", "VRMM", "Indian/Maldives"),
    ("Kathmandu Tribhuvan Intl", "KTM", "VNKT", "Asia/Kathmandu"),
    ("Dhaka Hazrat Shahjalal Intl", "DAC", "VGHS", "Asia/Dhaka"),
    ("Yangon International Airport", "RGN", "VYYY", "Asia/Yangon"),
    ("Hanoi Noi Bai International", "HAN", "VVNB", "Asia/Bangkok"),
    ("Ho Chi Minh Tan Son Nhat Intl", "SGN", "VVTS", "Asia/Bangkok"),
    ("Phnom Penh International", "PNH", "VDPP", "Asia/Phnom_Penh"),
    ("Vientiane Wattay Intl", "VTE", "VLVT", "Asia/Vientiane"),
    ("Ulaanbaatar Chinggis Khaan Intl", "UBN", "ZMCK", "Asia/Ulaanbaatar")
]

airlines = [
    ("Oman Air", "WY", "OMA"),
    ("Delta Air Lines", "DL", "DAL"),
    ("Emirates", "EK", "UAE"),
    ("Air New Zealand", "NZ", "ANZ"),
    ("British Airways", "BA", "BAW"),
    ("Malaysia Airlines", "MH", "MAS"),
    ("SriLankan Airlines", "UL", "ALK"),
    ("American Airlines", "AA", "AAL"),
    ("Lufthansa", "LH", "DLH"),
    ("Air France", "AF", "AFR"),
    ("Qatar Airways", "QR", "QTR"),
    ("Singapore Airlines", "SQ", "SIA"),
    ("KLM Royal Dutch Airlines", "KL", "KLM"),
    ("Turkish Airlines", "TK", "THY"),
    ("United Airlines", "UA", "UAL"),
    ("Ethiopian Airlines", "ET", "ETH"),
    ("Thai Airways", "TG", "THA"),
    ("Japan Airlines", "JL", "JAL"),
    ("Cathay Pacific", "CX", "CPA"),
    ("Qantas", "QF", "QFA"),
    ("Etihad Airways", "EY", "ETD"),
    ("Air India", "AI", "AIC"),
    ("IndiGo", "6E", "IGO"),
    ("SpiceJet", "SG", "SEJ"),
    ("Vistara", "UK", "VTI"),
    ("Go First", "G8", "GOW"),
    ("AirAsia", "AK", "AXM"),
    ("Scoot", "TR", "TGW"),
    ("China Southern Airlines", "CZ", "CSN"),
    ("China Eastern Airlines", "MU", "CES"),
    ("Hainan Airlines", "HU", "CHH"),
    ("Korean Air", "KE", "KAL"),
    ("Asiana Airlines", "OZ", "AAR"),
    ("Vietnam Airlines", "VN", "HVN"),
    ("Philippine Airlines", "PR", "PAL"),
    ("Bangkok Airways", "PG", "BKP"),
    ("Garuda Indonesia", "GA", "GIA"),
    ("Air Canada", "AC", "ACA"),
    ("WestJet", "WS", "WJA"),
    ("Avianca", "AV", "AVA"),
    ("LATAM Airlines", "LA", "LAN"),
    ("Azul Brazilian Airlines", "AD", "AZU"),
    ("Gol Linhas Aéreas", "G3", "GLO"),
    ("Aeromexico", "AM", "AMX"),
    ("Alaska Airlines", "AS", "ASA"),
    ("JetBlue Airways", "B6", "JBU"),
    ("Southwest Airlines", "WN", "SWA"),
    ("Ryanair", "FR", "RYR"),
    ("easyJet", "U2", "EZY"),
    ("Wizz Air", "W6", "WZZ")
]

flight_statuses = ["active", "landed", "cancelled", "scheduled", "delayed"]

# ----------------------------------------
# Helper: Generate Codeshare
# ----------------------------------------

def maybe_generate_codeshare(main_airline):
    if random.random() < 0.5:
        possible_partners = [a for a in airlines if a != main_airline]
        codeshare_airline = random.choice(possible_partners)
        flight_number = random.randint(100, 9999)
        return {
            "airline_name": codeshare_airline[0].lower(),
            "airline_iata": codeshare_airline[1].lower(),
            "airline_icao": codeshare_airline[2].lower(),
            "flight_number": str(flight_number),
            "flight_iata": f"{codeshare_airline[1].lower()}{flight_number}",
            "flight_icao": f"{codeshare_airline[2].lower()}{flight_number}"
        }
    return None

# ----------------------------------------
# Helper: Generate Aircraft Info
# ----------------------------------------

def generate_aircraft():
    aircraft_models = [
        ("Airbus", "A320-200", "A320", "A320"),
        ("Boeing", "737-800", "738", "B738"),
        ("Boeing", "777-300ER", "77W", "B77W"),
        ("Embraer", "E195-E2", "E95", "E295"),
        ("Bombardier", "CRJ900", "CR9", "CRJ9"),
        ("ATR", "72-600", "AT7", "AT76"),
    ]
    manufacturer, model, iata, icao = random.choice(aircraft_models)
    reg_prefixes = ["VT", "N", "G", "9V", "B", "JA", "HL", "HS", "PK"]
    registration = f"{random.choice(reg_prefixes)}-{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=3))}"
    icao24 = ''.join(random.choices("0123456789ABCDEF", k=6)).lower()
    return {
        "registration": registration,
        "iata": iata,
        "icao": icao,
        "icao24": icao24,
        "manufacturer": manufacturer,
        "model": model
    }

# ----------------------------------------
# Main Flight Record Generator
# ----------------------------------------

def generate_flight_record(flight_date: str):
    dep, arr = random.sample(airports, 2)
    airline = random.choice(airlines)

    # Determine realistic status based on date
    today = datetime.now(timezone.utc).date()
    record_date = datetime.strptime(flight_date, "%Y-%m-%d").date()

    if record_date < today:
        status = random.choices(
            ["landed", "cancelled", "delayed"], weights=[0.8, 0.1, 0.1]
        )[0]
    elif record_date == today:
        status = random.choices(
            ["active", "landed", "cancelled", "scheduled", "delayed"],
            weights=[0.3, 0.2, 0.1, 0.3, 0.1]
        )[0]
    else:
        status = random.choices(
            ["scheduled", "cancelled"], weights=[0.95, 0.05]
        )[0]

    flight_number = random.randint(100, 9999)
    dep_time = datetime.strptime(flight_date, "%Y-%m-%d") + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    arr_time = dep_time + timedelta(hours=random.randint(2, 12))

    return {
        "flight_date": flight_date,
        "flight_status": status,
        "departure": {
            "airport": dep[0],
            "timezone": dep[3],
            "iata": dep[1],
            "icao": dep[2],
            "terminal": random.choice(["1", "2", "3", "I", "M"]),
            "gate": random.choice(["A1", "B2", "C3", "D4"]) if status == "landed" else None,
            "delay": random.choice([None, random.randint(5, 60)]) if status in ["delayed", "landed"] else None,
            "scheduled": dep_time.isoformat() + "+00:00",
            "estimated": dep_time.isoformat() + "+00:00",
            "actual": dep_time.isoformat() + "+00:00" if status == "landed" else None,
            "estimated_runway": dep_time.isoformat() + "+00:00" if status == "landed" else None,
            "actual_runway": dep_time.isoformat() + "+00:00" if status == "landed" else None
        },
        "arrival": {
            "airport": arr[0],
            "timezone": arr[3],
            "iata": arr[1],
            "icao": arr[2],
            "terminal": random.choice(["1", "2", "3", "M"]),
            "gate": random.choice(["A5", "B6", "C7", "D8"]) if status == "landed" else None,
            "baggage": random.choice(["5", "8", "12", "22"]) if status == "landed" else None,
            "scheduled": arr_time.isoformat() + "+00:00",
            "delay": random.choice([None, random.randint(5, 60)]) if status in ["delayed", "landed"] else None,
            "estimated": arr_time.isoformat() + "+00:00" if status == "landed" else None,
            "actual": arr_time.isoformat() + "+00:00" if status == "landed" else None,
            "estimated_runway": arr_time.isoformat() + "+00:00" if status == "landed" else None,
            "actual_runway": arr_time.isoformat() + "+00:00" if status == "landed" else None
        },
        "airline": {
            "name": airline[0],
            "iata": airline[1],
            "icao": airline[2]
        },
        "flight": {
            "number": str(flight_number),
            "iata": f"{airline[1]}{flight_number}",
            "icao": f"{airline[2]}{flight_number}",
            "codeshared": maybe_generate_codeshare(airline)
        },
        "aircraft": generate_aircraft(),
        "live": None
    }


# ----------------------------------------
# Generator Runner
# ----------------------------------------

input_file_path = "/Users/parthmac/Desktop/Projects/Flight Data Analysis/data/raw data /flights_raw_data.json"
output_file_path = "/Users/parthmac/Desktop/Projects/Flight Data Analysis/data/raw data /flights_raw_data.json"
number_of_new_records = 10000
flight_date = "2025-08-10"

# Load existing data
with open(input_file_path, "r") as f:
    existing_data = json.load(f)

# Generate and append new records
synthetic_records = [generate_flight_record(flight_date) for _ in range(number_of_new_records)]
existing_data["data"].extend(synthetic_records)

# Save updated JSON
with open(output_file_path, "w") as f:
    json.dump(existing_data, f, indent=4)

print(f"✅ Appended {number_of_new_records} synthetic records to {output_file_path}")
