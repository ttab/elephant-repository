document "core/planning-item" {
  statuses = [
    "draft",
    "done",
    "cancelled",
    "usable",
  ]

  time_expression {
    expression = ".meta(type='core/planning-item').data{start_date:date, end_date:date, tz=date_tz?}"
  }

  time_expression {
    expression = ".meta(type='core/assignment').data{start_date:date, end_date:date, tz=date_tz?}"
  }

  time_expression {
    expression = ".meta(type='core/assignment').data{start?, publish?}"
  }

  label_expression {
    expression = ".links(rel='section')@{uuid}"
    template   = "section:{{.uuid.Value}}"
  }
}
