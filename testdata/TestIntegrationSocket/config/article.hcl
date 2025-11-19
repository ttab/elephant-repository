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

  workflow = {
    step_zero  = "draft"
    checkpoint = "usable"
    negative_checkpoint = "unpublished"
    steps = [
      "draft",
      "done",
      "cancelled",
    ]
  }
}

metric "charcount" {
  aggregation = "replace"
}
