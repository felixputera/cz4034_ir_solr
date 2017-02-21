import json
import os
import requests


def read_from_file(dir_folder='./data/'):
    text_data = []
    for files in os.listdir(dir_folder):
        with open(os.path.join(dir_folder, files)) as opened_file:
            text_data.append(opened_file.read())
    return text_data


def add_to_dict(posting):
    post_dict = {}
    try:
        post_dict['id'] = posting['id']

        try:
            post_dict['name_t'] = posting['name']
        except LookupError:
            print('no name in this post')

        try:
            post_dict['message_t'] = posting['message']
        except LookupError:
            print('no message in this post')

        try:
            post_dict['type_s'] = posting['type']
        except LookupError:
            print('no type in this post')

        try:
            post_dict['from_s'] = posting['from']['name']
        except LookupError:
            print('no publisher in this post')

        try:
            post_dict['share_count_i'] = posting['shares']['count']
        except LookupError:
            print('no share count in this post')

        try:
            post_dict['time_dt'] = posting['updated_time']
        except LookupError:
            print('no update time in this post')

        try:
            post_dict['desc_t'] = posting['description']
        except LookupError:
            print('no description in this post')

    except LookupError:
        print('invalid post')
    return post_dict


def send_to_solr(body_payload, core_name):
    print(json.dumps(body_payload))
    r = requests.post("http://localhost:8983/solr/{core}/update".format(core=core_name),
                      headers={"Content-Type": "application/json"},
                      data='{}'.format(json.dumps(body_payload)))
    print(r.json())


if __name__ == '__main__':
    text_data = read_from_file()
    for text in text_data:
        temp_json = json.loads(text)
        for post in temp_json['data']:
            to_be_posted = add_to_dict(post)

            payload = json.loads(''' {
                "add": {"doc" : %s,
                "commitWithin": 1000
            }}''' % json.dumps(to_be_posted))
            send_to_solr(payload, 'travelsearch')

