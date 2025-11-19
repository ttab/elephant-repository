document "core/article" {
  meta_doc = "core/article+meta"

  statuses = [
    "draft",
    "done",
    "approved",
    "withheld",
    "cancelled",
    "usable",
  ]
}

metric "charcount" {
  aggregation = "replace"
}
