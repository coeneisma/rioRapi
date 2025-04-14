# # Detecteer de structuur
# structure <- rio_detect_structure()
#
# # Extraheer relaties uit de structuur
# relations <- list()
# for (rel_name in names(structure$relationships)) {
#   detected_rel <- structure$relationships[[rel_name]]
#
#   # Maak een relatie-object
#   relation <- rio_create_relation(
#     from = detected_rel$source,
#     to = detected_rel$target,
#     type = "one-to-many",  # aanpassen indien nodig
#     from_field = detected_rel$exact_matches[1],  # eerste match gebruiken
#     to_field = detected_rel$exact_matches[1]
#   )
#
#   # Voeg toe aan relaties
#   relations <- rio_add_relation(relations, relation)
# }
#
# # Bewerk relaties
# relations <- rio_remove_relation(relations, "tabel1", "tabel2")
# new_relation <- rio_create_relation("tabel1", "tabel2", "one-to-many", "ID1", "ID2")
# relations <- rio_add_relation(relations, new_relation)
#
# # Exporteer naar CSV voor bewerking
# rio_export_relations_csv(relations, "rio_relations.csv")
#
# # Later: importeer bijgewerkte relaties
# relations <- rio_import_relations_csv("rio_relations_edited.csv")
#
# # Sla op als YAML
# rio_save_relations_yaml(relations)
#
# # Gebruik de relaties bij het combineren van datasets
# data1 <- rio_get_data(dataset_name = "tabel1")
# data2 <- rio_get_data(dataset_name = "tabel2")
# combined <- rio_combine_with_relations(relations, tabel1 = data1, tabel2 = data2)
