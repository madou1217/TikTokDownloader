<template>
  <div class="pager">
    <span class="pager-info">共 {{ total }} 条</span>
    <button class="ghost" :disabled="page <= 1" @click="go(page - 1)">
      上一页
    </button>
    <span class="pager-info">{{ page }} / {{ totalPages }}</span>
    <button class="ghost" :disabled="page >= totalPages" @click="go(page + 1)">
      下一页
    </button>
  </div>
</template>

<script setup>
import { computed } from "vue";

const props = defineProps({
  page: {
    type: Number,
    default: 1,
  },
  pageSize: {
    type: Number,
    default: 20,
  },
  total: {
    type: Number,
    default: 0,
  },
});

const emit = defineEmits(["change"]);

const totalPages = computed(() => {
  return Math.max(1, Math.ceil(props.total / props.pageSize));
});

const go = (value) => {
  const next = Math.min(Math.max(1, value), totalPages.value);
  if (next !== props.page) {
    emit("change", next);
  }
};
</script>
