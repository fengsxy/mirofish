<template>
  <div class="step-research">
    <!-- State: researching -->
    <div v-if="state === 'researching'" class="research-progress">
      <div class="section-header">
        <span class="header-icon">&#9670;</span>
        <span class="header-label">AUTO-RESEARCH</span>
      </div>
      <h2 class="section-title">正在自动检索证据</h2>
      <p class="section-desc">{{ phaseText }}</p>
      <div class="progress-bar-wrapper">
        <div class="progress-bar">
          <div class="progress-fill" :style="{ width: progressPercent + '%' }"></div>
        </div>
        <span class="progress-label">{{ progressPercent }}%</span>
      </div>
    </div>

    <!-- State: review -->
    <div v-else-if="state === 'review'" class="research-review">
      <div class="section-header">
        <span class="header-icon">&#9670;</span>
        <span class="header-label">EVIDENCE REVIEW</span>
      </div>
      <h2 class="section-title">检索结果确认</h2>
      <p class="section-desc">共找到 {{ evidenceList.length }} 条证据来源，请选择要纳入分析的内容</p>

      <div class="toolbar">
        <button class="toolbar-btn" @click="selectAll">全选</button>
        <button class="toolbar-btn" @click="selectNone">全不选</button>
        <span class="toolbar-count">已选 {{ selectedIds.size }} / {{ evidenceList.length }}</span>
      </div>

      <div class="evidence-cards">
        <label
          v-for="item in evidenceList"
          :key="item.id"
          class="evidence-card"
          :class="{ selected: selectedIds.has(item.id) }"
        >
          <input
            type="checkbox"
            :checked="selectedIds.has(item.id)"
            @change="toggleSelection(item.id)"
            class="card-checkbox"
          />
          <div class="card-body">
            <div class="card-title">{{ item.title || item.source || 'Untitled' }}</div>
            <div class="card-snippet">{{ item.snippet || item.summary || '' }}</div>
            <div v-if="item.url" class="card-url">{{ item.url }}</div>
          </div>
        </label>
      </div>

      <div class="extra-section">
        <label class="extra-label">补充说明（可选）</label>
        <textarea
          v-model="extraText"
          class="extra-textarea"
          placeholder="// 如有额外背景信息，可在此补充..."
          rows="3"
        ></textarea>
      </div>

      <button class="confirm-btn" @click="handleConfirm" :disabled="confirming || selectedIds.size === 0">
        <span v-if="!confirming">确认并继续 &#8594;</span>
        <span v-else>提交中...</span>
      </button>
    </div>

    <!-- State: error -->
    <div v-else-if="state === 'error'" class="research-error">
      <div class="section-header">
        <span class="header-icon error-icon">&#9632;</span>
        <span class="header-label">ERROR</span>
      </div>
      <h2 class="section-title">检索失败</h2>
      <p class="error-message">{{ errorMessage }}</p>
      <button class="retry-btn" @click="startResearchFlow">重试</button>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onUnmounted } from 'vue'
import { startResearch, getResearchStatus, getResearchResults, confirmResearch } from '../api/research'

const props = defineProps({
  projectData: {
    type: Object,
    required: true
  }
})

const emit = defineEmits(['research-confirmed'])

const state = ref('researching') // 'researching' | 'review' | 'error'
const phaseText = ref('初始化检索任务...')
const progressPercent = ref(0)
const taskId = ref(null)
const evidenceList = ref([])
const selectedIds = reactive(new Set())
const extraText = ref('')
const errorMessage = ref('')
const confirming = ref(false)

let pollTimer = null

const selectAll = () => {
  evidenceList.value.forEach(item => selectedIds.add(item.id))
}

const selectNone = () => {
  selectedIds.clear()
}

const toggleSelection = (id) => {
  if (selectedIds.has(id)) {
    selectedIds.delete(id)
  } else {
    selectedIds.add(id)
  }
}

const startResearchFlow = async () => {
  state.value = 'researching'
  phaseText.value = '初始化检索任务...'
  progressPercent.value = 0
  errorMessage.value = ''

  try {
    const question = props.projectData.simulation_requirement || props.projectData.question || ''
    const projectId = props.projectData.project_id || undefined
    const res = await startResearch(question, projectId)
    if (res.success || res.data) {
      taskId.value = res.data?.task_id || res.task_id
      startPolling()
    } else {
      state.value = 'error'
      errorMessage.value = res.error || 'Failed to start research'
    }
  } catch (err) {
    state.value = 'error'
    errorMessage.value = err.message || 'Failed to start research'
  }
}

const startPolling = () => {
  stopPolling()
  pollTimer = setInterval(pollStatus, 2000)
}

const stopPolling = () => {
  if (pollTimer) {
    clearInterval(pollTimer)
    pollTimer = null
  }
}

const pollStatus = async () => {
  if (!taskId.value) return
  try {
    const res = await getResearchStatus(taskId.value)
    const data = res.data || res
    progressPercent.value = data.progress || 0
    phaseText.value = data.phase || data.message || '检索中...'

    if (data.status === 'completed') {
      stopPolling()
      await loadResults()
    } else if (data.status === 'failed') {
      stopPolling()
      state.value = 'error'
      errorMessage.value = data.error || 'Research task failed'
    }
  } catch (err) {
    stopPolling()
    state.value = 'error'
    errorMessage.value = err.message || 'Status polling failed'
  }
}

const loadResults = async () => {
  try {
    const res = await getResearchResults(taskId.value)
    const data = res.data || res
    evidenceList.value = data.results || data.evidence || []
    // Default: select all
    selectedIds.clear()
    evidenceList.value.forEach(item => selectedIds.add(item.id))
    state.value = 'review'
  } catch (err) {
    state.value = 'error'
    errorMessage.value = err.message || 'Failed to load results'
  }
}

const handleConfirm = async () => {
  confirming.value = true
  try {
    const res = await confirmResearch(taskId.value, Array.from(selectedIds), extraText.value)
    const data = res.data || res
    emit('research-confirmed', data)
  } catch (err) {
    state.value = 'error'
    errorMessage.value = err.message || 'Failed to confirm research'
  } finally {
    confirming.value = false
  }
}

onMounted(() => {
  startResearchFlow()
})

onUnmounted(() => {
  stopPolling()
})
</script>

<style scoped>
.step-research {
  height: 100%;
  overflow-y: auto;
  padding: 32px;
  font-family: 'Space Grotesk', 'Noto Sans SC', system-ui, sans-serif;
  color: #000;
}

.section-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 16px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.8rem;
  color: #999;
}

.header-icon {
  color: #FF4500;
  font-size: 0.9rem;
}

.header-icon.error-icon {
  color: #F44336;
}

.header-label {
  letter-spacing: 1px;
  font-weight: 600;
}

.section-title {
  font-size: 1.6rem;
  font-weight: 520;
  margin: 0 0 8px 0;
}

.section-desc {
  color: #666;
  margin-bottom: 24px;
  line-height: 1.6;
  font-size: 0.95rem;
}

/* Progress */
.progress-bar-wrapper {
  display: flex;
  align-items: center;
  gap: 12px;
}

.progress-bar {
  flex: 1;
  height: 6px;
  background: #EAEAEA;
  border-radius: 3px;
  overflow: hidden;
}

.progress-fill {
  height: 100%;
  background: #FF4500;
  border-radius: 3px;
  transition: width 0.4s ease;
}

.progress-label {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.8rem;
  color: #999;
  min-width: 40px;
  text-align: right;
}

/* Toolbar */
.toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
}

.toolbar-btn {
  border: 1px solid #DDD;
  background: #FFF;
  padding: 6px 16px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  color: #333;
}

.toolbar-btn:hover {
  border-color: #FF4500;
  color: #FF4500;
}

.toolbar-count {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  color: #999;
  margin-left: auto;
}

/* Evidence Cards */
.evidence-cards {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-bottom: 24px;
  max-height: 400px;
  overflow-y: auto;
}

.evidence-card {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  border: 1px solid #E5E5E5;
  padding: 16px;
  cursor: pointer;
  transition: all 0.2s;
  background: #FFF;
}

.evidence-card:hover {
  border-color: #CCC;
  background: #FAFAFA;
}

.evidence-card.selected {
  border-color: #FF4500;
  background: #FFF8F5;
}

.card-checkbox {
  margin-top: 2px;
  accent-color: #FF4500;
  cursor: pointer;
}

.card-body {
  flex: 1;
  min-width: 0;
}

.card-title {
  font-weight: 600;
  font-size: 0.95rem;
  margin-bottom: 4px;
  color: #000;
}

.card-snippet {
  font-size: 0.85rem;
  color: #666;
  line-height: 1.5;
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.card-url {
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.7rem;
  color: #999;
  margin-top: 6px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* Extra text */
.extra-section {
  margin-bottom: 24px;
}

.extra-label {
  display: block;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.75rem;
  color: #999;
  margin-bottom: 8px;
}

.extra-textarea {
  width: 100%;
  border: 1px solid #DDD;
  background: #FAFAFA;
  padding: 12px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.85rem;
  line-height: 1.6;
  resize: vertical;
  outline: none;
  box-sizing: border-box;
}

.extra-textarea:focus {
  border-color: #FF4500;
}

/* Confirm button */
.confirm-btn {
  width: 100%;
  background: #000;
  color: #FFF;
  border: 1px solid #000;
  padding: 16px;
  font-family: 'JetBrains Mono', monospace;
  font-weight: 700;
  font-size: 1rem;
  cursor: pointer;
  transition: all 0.3s ease;
  letter-spacing: 1px;
}

.confirm-btn:hover:not(:disabled) {
  background: #FF4500;
  border-color: #FF4500;
}

.confirm-btn:disabled {
  background: #E5E5E5;
  color: #999;
  border-color: #E5E5E5;
  cursor: not-allowed;
}

/* Error */
.error-message {
  color: #F44336;
  margin-bottom: 20px;
  font-size: 0.9rem;
  line-height: 1.5;
  padding: 12px;
  border: 1px solid #FFCDD2;
  background: #FFF5F5;
}

.retry-btn {
  border: 1px solid #000;
  background: #FFF;
  padding: 10px 24px;
  font-family: 'JetBrains Mono', monospace;
  font-weight: 600;
  font-size: 0.85rem;
  cursor: pointer;
  transition: all 0.2s;
}

.retry-btn:hover {
  background: #000;
  color: #FFF;
}
</style>
