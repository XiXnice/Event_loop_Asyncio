import asyncio
import aiohttp
import datetime

from more_itertools import chunked

from models import init_db, People, Session

CHUNK_SIZE = 10


async def paste_to_db(people, people_id_chunk):
    async with Session() as session:
        people = [People(id=int(id_chunk),
                         birth_year=str(person["birth_year"]),
                         eye_color=str(person["eye_color"]),
                         films=str(person["films"]),
                         gender=str(person["gender"]),
                         hair_color=str(person["hair_color"]),
                         height=str(person["height"]),
                         homeworld=str(person["homeworld"]),
                         mass=str(person["mass"]),
                         name=str(person["name"]),
                         skin_color=str(person["skin_color"]),
                         species=str(person["species"]),
                         starships=str(person["starships"]),
                         vehicles=str(person["vehicles"]), ) for person, id_chunk in zip(people, people_id_chunk)]
        session.add_all(people)
        await session.commit()


async def get_person(person_id, session):
    response = await session.get(f'https://swapi.py4e.com/api/people/{person_id}/')
    json = await response.json()
    return json


async def main():
    await init_db()

    async with aiohttp.ClientSession() as session:
        for people_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
            coros = [get_person(people_id, session) for people_id in people_id_chunk]
            result = await asyncio.gather(*coros)
            result = await converter(result)
            asyncio.create_task(paste_to_db(result, people_id_chunk))

    tasks_to_await = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*tasks_to_await)


async def converter(result):
    final_list = []
    for dict_pep in result:
        for key, value_list in dict_pep.items():
            if type(value_list) == list:
                string_set = ', '.join(str(value) for value in value_list)
                dict_pep.update({key: string_set})
        final_list.append(dict_pep)

    return final_list


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
