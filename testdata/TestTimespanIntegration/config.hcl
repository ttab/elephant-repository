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
    steps      = [
      "draft",
      "done",
      "approved",
      "withheld",
      "cancelled",
    ]
  }
}

metric "charcount" {
  aggregation = "replace"
}

document "core/planning-item" {
  statuses = [
    "draft",
    "done",
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

  time_expression {
    expression = ".meta(type='core/planning-item').data{start_date:date, end_date:date, tz=date_tz?}"
  }

  time_expression {
    expression = ".meta(type='core/assignment').data{start_date:date, end_date:date, tz=date_tz?}"
  }

  time_expression {
    expression = ".meta(type='core/assignment').data{start?, publish?}"
  }
}

document "core/event" {
  statuses = [
    "draft",
    "done",
    "cancelled",
    "usable",    
  ]

  workflow = {
    step_zero  = "draft"
    checkpoint = "usable"
    negative_checkpoint = "unpublished"
    steps      = [
      "draft",
      "done",
      "cancelled",
    ]
  }

  time_expression {
    expression = ".meta(type='core/event').data{start, end}"
  }
}
