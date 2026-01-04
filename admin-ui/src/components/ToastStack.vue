<template>
  <div class="toast-stack">
    <div v-for="toast in toasts" :key="toast.id" :class="['toast', toast.type]">
      <span class="toast-text">{{ toast.message }}</span>
    </div>
  </div>
</template>

<script setup>
defineProps({
  toasts: {
    type: Array,
    default: () => [],
  },
});
</script>
