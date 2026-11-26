import type { Hotspot } from './model';

export type HotspotView = 'leaf' | 'inclusive' | 'stacks';
export type CPUStack = { frames: string[]; samples: number };
export type CPUProfile = { stacks: CPUStack[]; samples: number };

export function parseFoldedCPU(text: string): CPUProfile {
  const stacks: CPUStack[] = [];
  let samples = 0;
  for (const line of text.trimEnd().split('\n')) {
    const separator = line.lastIndexOf(' ');
    const count = Number(line.slice(separator + 1));
    if (separator < 1 || !Number.isSafeInteger(count) || count < 1) throw new Error('CPU 样本归档格式无效');
    stacks.push({ frames: line.slice(0, separator).split(';'), samples: count });
    samples += count;
  }
  return { stacks, samples };
}

export function aggregateHotspots(profile: CPUProfile, view: HotspotView): Hotspot[] {
  const counts = new Map<string, number>();
  for (const { frames, samples } of profile.stacks) {
    const keys = view === 'stacks' ? [frames.join(';')] : view === 'leaf' ? frames.slice(-1) : new Set(frames);
    for (const key of keys) counts.set(key, (counts.get(key) ?? 0) + samples);
  }
  return [...counts].map(([name, samples]) => ({ function: name, samples, percent: 100 * samples / profile.samples }));
}

export function callersFor(profile: CPUProfile, view: HotspotView, hotspot: string): CPUStack[] {
  const paths = new Map<string, CPUStack>();
  for (const { frames, samples } of profile.stacks) {
    if (view === 'stacks' && frames.join(';') !== hotspot) continue;
    if (view === 'leaf' && frames.at(-1) !== hotspot) continue;
    const end = view === 'inclusive' ? frames.lastIndexOf(hotspot) + 1 : frames.length;
    if (!end) continue;
    const path = frames.slice(0, end);
    const key = path.join(';');
    const current = paths.get(key);
    if (current) current.samples += samples;
    else paths.set(key, { frames: path, samples });
  }
  return [...paths.values()].sort((a, b) => b.samples - a.samples);
}
