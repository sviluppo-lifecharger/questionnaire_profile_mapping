from pymongo import MongoClient
from bson.objectid import ObjectId
import json
import os

client = MongoClient(
        'mongodb+srv://claudio:Life10293@lifechargercluster00.cvcqa.mongodb.net/test?authSource=admin&replicaSet=atlas-6ol9kl-shard-0&readPreference=primary&ssl=true')
db = client['GlobalData']
questionnaire_mapping_collection = db['questionnaire_mapping']
profile_modules_mapping_collection = db['profile_module_mapping']

questionnaires_collection = db["questionnaire_template_root"]
questionnaires_questions_collection = db["questionnaire_template_question"]
questionnaires_answers_collection = db["questionnaire_template_answer"]

profile_modules_collection = db["profile_template_modules"]
profile_questions_collection = db["profile_template_questions"]
profile_answers_collection = db["profile_template_answers"]

questionnaire_mapping_records = []
profile_mapping_records = []

#estraggo la lista degli unique_template_id dei questionari senza duplicati
unique_questionnaire_map_ids = []
for doc in questionnaires_collection.find():
        unique_template_map_id = doc.get("related_unique_map_id")
        unique_questionnaire_map_ids.append(unique_template_map_id)
print(unique_questionnaire_map_ids)
unique_questionnaire_map_ids = set(unique_questionnaire_map_ids)

#genera i record della mappatura questionari
for i in unique_questionnaire_map_ids:
        print("i: ", i )
        questions = []
        answers = []
        root_map = []
        for doc in questionnaires_collection.find():
                if doc.get("related_unique_map_id") == i:
                        print("unique_id::: ", doc.get("related_unique_map_id"), "I DEL FOR: ", i)
                        print("trovato match nel db per l'id: ", i)
                        counter = 0
                        #unique_template_id = ObjectId()
                        unique_template_id = doc.get("related_unique_map_id")
                        #da cambiare con un codice generato quest_map_0001
                        root_json = {
                                "template_id" : doc.get("template_id"),
                                "language": doc.get("language")
                        }
                        print("ROOT JSON: ", root_json)
                        root_map.append(root_json)
                        print("ROOT MAP", root_map)

                        for domanda in doc.get("questions"):
                                question_position = counter
                                domanda = str(domanda)
                                for d in questionnaires_questions_collection.find():
                                        if d.get("template_id") == domanda:
                                                domanda_json = {
                                                        "language": doc.get("language"),
                                                        "position": str(question_position),
                                                        "template_id": d.get("template_id"),
                                                        #"text": d.get("question_text"),
                                                        "unique_question_id": f"{unique_template_id}, {question_position}"
                                                }
                                                questions.append(domanda_json)
                                                counter2 = 0
                                                for risposta in d.get("answers"):
                                                        answer_position = str(question_position) + "." + str(counter2)
                                                        risposta = str(risposta)
                                                        for ans in questionnaires_answers_collection.find():
                                                                if ans.get("template_id") == risposta:
                                                                        risposta_json = {
                                                                                "language":  doc.get("language"),
                                                                                "position": answer_position,
                                                                                "template_id": ans.get("template_id"),
                                                                                #"text": ans.get("answer_text"),
                                                                                "unique_answer_template_id": f"{unique_template_id}, {answer_position}"
                                                                        }
                                                                        answers.append(risposta_json)
                                                                        counter3 = 0
                                                                        #'''
                                                                        if ans.get('open_next_answers'):
                                                                                print("la risposta ha delle sottodomande")
                                                                                print(ans.get("template_id"))
                                                                        #if ans.get("next_answers"):
                                                                                for sub_q in ans.get("next_answers"):
                                                                                        subquestion_position = str(
                                                                                                question_position) + "." + str(
                                                                                                counter2) + "." + str(
                                                                                                counter3)
                                                                                        sub_q = str(sub_q)
                                                                                        for subq in questionnaires_questions_collection.find():
                                                                                                if subq.get("template_id") == sub_q:
                                                                                                        domanda_json = {
                                                                                                                "language": doc.get(
                                                                                                                        "language"),
                                                                                                                "position": str(
                                                                                                                        subquestion_position),
                                                                                                                "template_id": subq.get(
                                                                                                                        "template_id"),
                                                                                                                # "text": d.get("question_text"),
                                                                                                                "unique_question_id": f"{unique_template_id}, {subquestion_position}"
                                                                                                        }
                                                                                                        questions.append(
                                                                                                                domanda_json)
                                                                                                        counter4 = 0
                                                                                                        for sub_r in subq.get(
                                                                                                                "answers"):
                                                                                                                subanswer_position = str(
                                                                                                                        question_position) + "." + str(
                                                                                                                        counter2) + "." + str(
                                                                                                                        counter3) + "." + str(
                                                                                                                        counter4)
                                                                                                                #print("sottorisposta",
                                                                                                                #      subanswer_position)
                                                                                                                sub_r = str(
                                                                                                                        sub_r)
                                                                                                                #print("sotto risposta id",
                                                                                                                #      sub_r)
                                                                                                                for sub_a in questionnaires_answers_collection.find():
                                                                                                                        # print("sub risposta", sub_r)
                                                                                                                        if sub_a.get(
                                                                                                                                "template_id") == sub_r:
                                                                                                                                #print("template trovato")
                                                                                                                                risposta_json = {
                                                                                                                                        "language": doc.get(
                                                                                                                                                "language"),
                                                                                                                                        "position": subanswer_position,
                                                                                                                                        "template_id": sub_a.get(
                                                                                                                                                "template_id"),
                                                                                                                                        # "text": ans.get("answer_text"),
                                                                                                                                        "unique_answer_template_id": f"{unique_template_id}, {subanswer_position}"

                                                                                                                                }
                                                                                                                                # print(risposta_json)
                                                                                                                                answers.append(
                                                                                                                                        risposta_json)
                                                                                                                counter4 += 1
                                                                                        counter3 += 1
                                                                        #'''
                                                        counter2 += 1
                                counter += 1
                        #print("domande: ", questions)
                        #print("risposte", answers)

        record = {
                "map_version": "1.0.1",
                "unique_template_id": str(unique_template_id),
                "root_map": root_map,
                "questions_map": questions,
                "answers_map": answers
        }

        #print("record: ", record)
        questionnaire_mapping_records.append(record)


#estraggo la lista degli unique_template_id dei profili senza duplicati
unique_profile_map_ids = []
for doc in profile_modules_collection.find():
        unique_template_map_id = doc.get("related_unique_map_id")
        unique_profile_map_ids.append(unique_template_map_id)
print("unique_profile_ids", unique_profile_map_ids)
unique_profile_map_ids = set(unique_profile_map_ids)

#genera i record della mappatura profili
for i in unique_profile_map_ids:
        print(i)
        questions = []
        answers = []
        root_map = []
        for doc in profile_modules_collection.find():
                if doc.get("related_unique_map_id") == i:
                        counter = 0
                        unique_template_id = doc.get("related_unique_map_id")
                        root_json = {
                                "template_id": doc.get("template_id"),
                                "language": doc.get("language")
                        }
                        root_map.append(root_json)
                        print("root_map", root_map)

                        for domanda in doc.get("q"):
                                question_position = counter
                                domanda = str(domanda)
                                for d in profile_questions_collection.find():
                                        if d.get("template_id") == domanda:
                                                domanda_json = {
                                                        "language": doc.get("language"),
                                                        "position": str(question_position),
                                                        "template_id": d.get("template_id"),
                                                        # "text": d.get("question_text"),
                                                        "unique_question_id": f"{unique_template_id}, {question_position}"
                                                }
                                                questions.append(domanda_json)
                                                counter2 = 0
                                                for risposta in d.get("ans"):
                                                        answer_position = str(question_position) + "." + str(counter2)
                                                        risposta = str(risposta)
                                                        for ans in profile_answers_collection.find():
                                                                if ans.get("template_id") == risposta:
                                                                        risposta_json = {
                                                                                "language": doc.get("language"),
                                                                                "position": answer_position,
                                                                                "template_id": ans.get("template_id"),
                                                                                # "text": ans.get("answer_text"),
                                                                                "unique_answer_template_id": f"{unique_template_id}, {answer_position}"
                                                                        }
                                                                        answers.append(risposta_json)
                                                                        counter3 = 0
                                                                        if ans.get("next_ans"):
                                                                                for sub_q in ans.get("next_ans"):
                                                                                        subquestion_position = str(
                                                                                                question_position) + "." + str(
                                                                                                counter2) + "." + str(
                                                                                                counter3)
                                                                                        sub_q = str(sub_q)
                                                                                        for subq in profile_questions_collection.find():
                                                                                                if subq.get(
                                                                                                        "template_id") == sub_q:
                                                                                                        domanda_json = {
                                                                                                                "language": doc.get(
                                                                                                                        "language"),
                                                                                                                "position": str(
                                                                                                                        subquestion_position),
                                                                                                                "template_id": subq.get(
                                                                                                                        "template_id"),
                                                                                                                # "text": d.get("question_text"),
                                                                                                                "unique_question_id": f"{unique_template_id}, {subquestion_position}"
                                                                                                        }
                                                                                                        questions.append(
                                                                                                                domanda_json)
                                                                                                        counter4 = 0
                                                                                                        for sub_r in subq.get(
                                                                                                                "ans"):
                                                                                                                subanswer_position = str(
                                                                                                                        question_position) + "." + str(
                                                                                                                        counter2) + "." + str(
                                                                                                                        counter3) + "." + str(
                                                                                                                        counter4)
                                                                                                                print("sottorisposta",
                                                                                                                      subanswer_position)
                                                                                                                sub_r = str(
                                                                                                                        sub_r)
                                                                                                                print("sotto risposta id",
                                                                                                                      sub_r)
                                                                                                                for i in profile_answers_collection.find():
                                                                                                                        # print("sub risposta", sub_r)
                                                                                                                        if i.get(
                                                                                                                                "template_id") == sub_r:
                                                                                                                                print("template trovato")
                                                                                                                                risposta_json = {
                                                                                                                                        "language": doc.get(
                                                                                                                                                "language"),
                                                                                                                                        "position": subanswer_position,
                                                                                                                                        "template_id": i.get(
                                                                                                                                                "template_id"),
                                                                                                                                        # "text": ans.get("answer_text"),
                                                                                                                                        "unique_answer_template_id":  f"{unique_template_id}, {subanswer_position}"
                                                                                                                                }
                                                                                                                                # print(risposta_json)
                                                                                                                                answers.append(
                                                                                                                                        risposta_json)
                                                                                                                                counter5 = 0
                                                                                                                                if i.get(
                                                                                                                                        "next_ans"):
                                                                                                                                        for subsubq in i.get(
                                                                                                                                                "next_ans"):
                                                                                                                                                subsubq_pos = str(
                                                                                                                                                        question_position) + "." + str(
                                                                                                                                                        counter2) + "." + str(
                                                                                                                                                        counter3) + "." + str(
                                                                                                                                                        counter4) + "." + str(
                                                                                                                                                        counter5)
                                                                                                                                                print("sotto sotto domanda",
                                                                                                                                                      subsubq_pos)
                                                                                                                                                subsubq = str(
                                                                                                                                                        subsubq)
                                                                                                                                                for element in profile_questions_collection.find():
                                                                                                                                                        if element.get(
                                                                                                                                                                "template_id") == subsubq:
                                                                                                                                                                domanda_json = {
                                                                                                                                                                        "language": doc.get(
                                                                                                                                                                                "language"),
                                                                                                                                                                        "position": str(
                                                                                                                                                                                subsubq_pos),
                                                                                                                                                                        "template_id": element.get(
                                                                                                                                                                                "template_id"),
                                                                                                                                                                        # "text": d.get("question_text"),
                                                                                                                                                                        "unique_question_id":  f"{unique_template_id}, {subsubq_pos}"
                                                                                                                                                                }
                                                                                                                                                                questions.append(
                                                                                                                                                                        domanda_json)
                                                                                                                                                                counter6 = 0
                                                                                                                                                                for subsubr in element.get(
                                                                                                                                                                        "ans"):
                                                                                                                                                                        subsubr_pos = str(
                                                                                                                                                                                question_position) + "." + str(
                                                                                                                                                                                counter2) + "." + str(
                                                                                                                                                                                counter3) + "." + str(
                                                                                                                                                                                counter4) + "." + str(
                                                                                                                                                                                counter5) + "." + str(
                                                                                                                                                                                counter6)
                                                                                                                                                                        print("sotto sotto risposta",
                                                                                                                                                                              subsubr_pos)
                                                                                                                                                                        subsubr = str(
                                                                                                                                                                                subsubr)
                                                                                                                                                                        for answer in profile_answers_collection.find():
                                                                                                                                                                                if answer.get(
                                                                                                                                                                                        "template_id") == subsubr:
                                                                                                                                                                                        risposta_json = {
                                                                                                                                                                                                "language": doc.get(
                                                                                                                                                                                                        "language"),
                                                                                                                                                                                                "position": subsubr_pos,
                                                                                                                                                                                                "template_id": answer.get(
                                                                                                                                                                                                        "template_id"),
                                                                                                                                                                                                # "text": ans.get("answer_text"),
                                                                                                                                                                                                "unique_answer_template_id": f"{unique_template_id}, {subsubr_pos}"
                                                                                                                                                                                        }
                                                                                                                                                                                        answers.append(
                                                                                                                                                                                                risposta_json)
                                                                                                                                                                        counter6 += 1
                                                                                                                                                counter5 += 1
                                                                                                                counter4 += 1
                                                                                        counter3 += 1
                                                        counter2 += 1
                                counter += 1
        record = {
                "map_version": "1.0.1",
                "unique_template_id": str(unique_template_id),
                "root_map": root_map,
                "questions_map": questions,
                "answers_map": answers
        }
        # print("record: ", record)
        profile_mapping_records.append(record)


#salvo ogni mappatura dei questionari
for i, rep in enumerate(questionnaire_mapping_records):
        print(rep['unique_template_id'])
        json_file = "record_mapping_" + rep['unique_template_id'] + "_.json"
        #db_report_list.append(rep)
        # json_file = f'report_{i+1}.json'
        with open("/Users/Sviluppo/mapping/" + json_file, 'w') as file:
                # with open("/Users/tommasofracchia/Desktop/LifeCharger/REPORTS/" + json_file, 'w') as file:
                json.dump(rep, file, indent=4)

#salvo ogni mappatura dei profili
for i, rep in enumerate(profile_mapping_records):
        print(rep['unique_template_id'])
        json_file = "record_mapping_" + rep['unique_template_id'] + "_.json"
        #db_report_list.append(rep)
        # json_file = f'report_{i+1}.json'
        with open("/Users/Sviluppo/mapping/" + json_file, 'w') as file:
                # with open("/Users/tommasofracchia/Desktop/LifeCharger/REPORTS/" + json_file, 'w') as file:
                json.dump(rep, file, indent=4)

#'''

folder_path = "/Users/Sviluppo/mapping/"

def process_file(file_path):
        # Inserisci qui la logica per elaborare il file JSON
        print(f"Elaborazione del file: {file_path}")
        # Leggi il contenuto del file JSON
        with open(file_path) as f:
                data = json.load(f)
                print(data)

        file_name = os.path.basename(file_path)
        print("file_name ", file_name)
        if file_name.startswith("record_mapping_quest"):
                print("entra")
                collection = db['questionnaire_mapping']
                #collection.insert_one(data)
                #'''
                unique_template_id = data.get("unique_template_id")
                if unique_template_id:
                        existing_report = collection.find_one({"unique_template_id": unique_template_id})
                        if existing_report:
                                collection.replace_one({"unique_template_id": unique_template_id}, data)
                        else:
                                collection.insert_one(data)
                else:
                        collection.insert_one(data)
                #        '''
        elif file_name.startswith("record_mapping_profile"):
                print("entra")
                collection = db['profile_module_mapping']
                #collection.insert_one(data)
                #'''
                unique_template_id = data.get("unique_template_id")
                if unique_template_id:
                        existing_report = collection.find_one({"unique_template_id": unique_template_id})
                        if existing_report:
                                collection.replace_one({"unique_template_id": unique_template_id}, data)
                        else:
                                collection.insert_one(data)
                else:
                        collection.insert_one(data)
                #'''

for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        print(os.path.isfile(file_path))
        if os.path.isfile(file_path) and filename.endswith(".json"):
                print("processa il record")
                process_file(file_path)