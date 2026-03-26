import service from './index'

export function startResearch(question, projectId) {
  return service({
    url: '/api/research/start',
    method: 'post',
    data: { question, project_id: projectId }
  })
}

export function getResearchStatus(taskId) {
  return service({
    url: `/api/research/status/${taskId}`,
    method: 'get'
  })
}

export function getResearchResults(taskId) {
  return service({
    url: `/api/research/results/${taskId}`,
    method: 'get'
  })
}

export function confirmResearch(taskId, selectedIds, extraText) {
  return service({
    url: `/api/research/confirm/${taskId}`,
    method: 'post',
    data: {
      selected_ids: selectedIds,
      extra_text: extraText
    }
  })
}
