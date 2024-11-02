import psycopg2
from auth import worksheet
from config import get_database_config
import pytz
import datetime

def checkNull(data_):
    for i in range(len(data_)):
        for j in range(len(data_[i])):
            if data_[i][j] == '':
                data_[i][j] = None

def checkComma(data_):
    for i in range(len(data_)):
        for j in range(len(data_[i])):
            if isinstance(data_[i][j], str) and '.' in data_[i][j]:
                data_[i][j] = data_[i][j].replace('.', '')

def removeByPositions(data_):
    positions_to_remove = [len(data_)]
    for item in data_:
        for position in sorted(positions_to_remove, reverse=True):
            if 0 <= position < len(item):
                item.pop(position)
def importDataFromSheets():
    timezone = pytz.timezone("Asia/Ho_Chi_Minh")
    # Read data
    data = worksheet.get_all_values()
    if data:
        header = data.pop(0)

    checkNull(data)
    # removeByPositions(data)

    # Connect DB
    params = get_database_config()

    connection = psycopg2.connect(**params)
    cur = connection.cursor()

    query = """
        insert into other_raw_data.campaign_tracking (id,description,code,campaign,source,content,total_user,start_date,end_date,funding) 
        values {}
        on conflict (id) do update
        set id=excluded.id, description=excluded.description, code=excluded.code, campaign=excluded.campaign,
            source = excluded.source, content=excluded.content, total_user=excluded.total_user,
            start_date=excluded.start_date, end_date=excluded.end_date, funding=excluded.funding
            returning *;
    """

    try:
        args_str = ','.join(
            cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)", i).decode('utf-8')
            for i in data)

        full_query = query.format(args_str)
        cur.execute(full_query)
        connection.commit()

        cur.execute("""create temp table temp_ids (id int)""")
        temp_ids = [(i[0],) for i in data]
        cur.executemany("""insert into temp_ids (id) values (%s)""", temp_ids)

        cur.execute("""delete from other_raw_data.campaign_tracking where id not in (select id from temp_ids)""")
        connection.commit()

        cur.execute("""drop table temp_ids""")
        connection.commit()

        current_time = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Successfully updated at: {current_time}")

    except Exception as e:
        connection.rollback()
        print('error: ', e)

    finally:
        if cur is not None:
            cur.close()
        if connection is not None:
            connection.close()
