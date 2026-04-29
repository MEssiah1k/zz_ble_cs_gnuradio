% Plot one GNU Radio complex64 capture and compensate per-burst CFO.
%
% Usage:
%   1. Set file_path below.
%   2. Run this script in MATLAB.
%   3. Inspect the selected burst before/after compensation.

clear; clc;

% Match the flowgraph sample rate. Current 1to1_2sides.grc uses 9.5e6.
fs = 9.5e6;

file_path = '/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_2m';

% Set [] to read the whole file. For quick debugging, use e.g. 5e7 float32
% values, which equals 2.5e7 complex samples.
max_float_count = [];

% Burst detection knobs. If too many noise segments are selected, increase
% threshold_ratio. If packets are missed, decrease it.
smooth_len = 256;
threshold_ratio = 0.20;
min_burst_len = 1000;
merge_gap = 500;
edge_trim = 200;

% Choose which detected burst to inspect.
selected_burst = 1;

fi = fopen(file_path, 'rb');
if fi < 0
    error('Cannot open file: %s', file_path);
end

if isempty(max_float_count)
    raw = fread(fi, 'float32');
else
    raw = fread(fi, max_float_count, 'float32');
end
fclose(fi);

if mod(numel(raw), 2) ~= 0
    raw = raw(1:end-1);
end

x = raw(1:2:end) + 1i * raw(2:2:end);
n_all = (0:numel(x)-1).';

amp = abs(x);
amp_smooth = movmean(amp, smooth_len);
noise_floor = prctile(amp_smooth, 20);
signal_level = prctile(amp_smooth, 99.5);
threshold = noise_floor + threshold_ratio * (signal_level - noise_floor);

mask = amp_smooth > threshold;
mask = merge_short_gaps(mask, merge_gap);
[starts, stops] = true_runs(mask);

lens = stops - starts + 1;
keep = lens >= min_burst_len;
starts = starts(keep);
stops = stops(keep);

fprintf('file: %s\n', file_path);
fprintf('complex samples: %d\n', numel(x));
fprintf('threshold: %.6g, bursts detected: %d\n', threshold, numel(starts));

if isempty(starts)
    error('No bursts detected. Try lowering threshold_ratio or min_burst_len.');
end
if selected_burst > numel(starts)
    error('selected_burst=%d, but only %d bursts detected.', selected_burst, numel(starts));
end

s0 = starts(selected_burst);
s1 = stops(selected_burst);
s0 = min(s1, s0 + edge_trim);
s1 = max(s0, s1 - edge_trim);

seg = x(s0:s1);
n = (0:numel(seg)-1).';

ph = unwrap(angle(seg));
p = polyfit(n, ph, 1);
phase_slope_rad_per_sample = p(1);
cfo_hz = phase_slope_rad_per_sample * fs / (2*pi);

seg_fix = seg .* exp(-1i * phase_slope_rad_per_sample * n);
ph_fix = unwrap(angle(seg_fix));

fprintf('selected burst: %d, sample range: [%d, %d], len=%d\n', ...
    selected_burst, s0, s1, numel(seg));
fprintf('estimated CFO: %.6f Hz\n', cfo_hz);
fprintf('phase std before/after detrend: %.6f / %.6f rad\n', ...
    std(ph - polyval(p, n)), std(ph_fix - mean(ph_fix)));

figure;
subplot(3, 1, 1);
plot(n_all, amp);
hold on;
yline(threshold, '--r');
xline(s0, '--g');
xline(s1, '--g');
title('Amplitude and selected burst');
xlabel('Sample');
ylabel('Abs');
grid on;

subplot(3, 1, 2);
plot(n, angle(seg));
title(sprintf('Before CFO compensation, burst %d', selected_burst));
xlabel('Sample in burst');
ylabel('Wrapped phase (rad)');
grid on;

subplot(3, 1, 3);
plot(n, angle(seg_fix));
title(sprintf('After CFO compensation, estimated CFO %.3f Hz', cfo_hz));
xlabel('Sample in burst');
ylabel('Wrapped phase (rad)');
grid on;

function merged = merge_short_gaps(mask, max_gap)
    merged = mask(:);
    idx = 1;
    while idx <= numel(merged)
        if merged(idx)
            idx = idx + 1;
            continue;
        end
        gap_start = idx;
        while idx <= numel(merged) && ~merged(idx)
            idx = idx + 1;
        end
        gap_stop = idx - 1;
        left_on = gap_start > 1 && merged(gap_start - 1);
        right_on = idx <= numel(merged) && merged(idx);
        if left_on && right_on && (gap_stop - gap_start + 1) <= max_gap
            merged(gap_start:gap_stop) = true;
        end
    end
end

function [starts, stops] = true_runs(mask)
    mask = mask(:);
    d = diff([false; mask; false]);
    starts = find(d == 1);
    stops = find(d == -1) - 1;
end
