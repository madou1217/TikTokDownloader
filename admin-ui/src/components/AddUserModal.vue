<template>
  <div v-if="open" class="modal-mask" @click.self="close">
    <div class="modal-card" role="dialog" aria-modal="true">
      <div class="modal-header">
        <h3>新增用户</h3>
      </div>
      <div class="modal-body">
        <label class="field">
          <span>用户标识或链接</span>
          <input
            v-model="form.secUserId"
            placeholder="支持用户标识/直播链接/用户主页链接"
          />
        </label>
      </div>
      <div class="modal-actions">
        <button class="ghost" @click="close">取消</button>
        <button class="primary" :disabled="!canSubmit || loading" @click="submit">
          <span v-if="loading" class="spinner"></span>
          {{ loading ? "新增中..." : "确认新增" }}
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, reactive, watch } from "vue";

const props = defineProps({
  open: {
    type: Boolean,
    default: false,
  },
  loading: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["update:open", "submit"]);

const form = reactive({
  secUserId: "",
});

const canSubmit = computed(() => form.secUserId.trim().length > 0);

const close = () => {
  emit("update:open", false);
};

const submit = () => {
  if (!canSubmit.value) {
    return;
  }
  emit("submit", form.secUserId);
};

watch(
  () => props.open,
  (value) => {
    if (value) {
      form.secUserId = "";
    }
  }
);
</script>
