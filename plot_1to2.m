clc
% clear all
close all

root_dir = '/home/lfy/workarea/zz_ble_cs_gnuradio/1to2';

captures = {
    'Calibration: initiator RX from reflectors', ...
    fullfile(root_dir, 'data_initiator_rx_from_reflectors_calibration');

    'Calibration: reflector1 RX from initiator', ...
    fullfile(root_dir, 'data_reflector1_rx_from_initiator_calibration');

    'Calibration: reflector2 RX from initiator', ...
    fullfile(root_dir, 'data_reflector2_rx_from_initiator_calibration');

    'Measurement: initiator RX from reflectors', ...
    fullfile(root_dir, 'data_initiator_rx_from_reflectors_measurement');

    'Measurement: reflector1 RX from initiator', ...
    fullfile(root_dir, 'data_reflector1_rx_from_initiator_measurement');

    'Measurement: reflector2 RX from initiator', ...
    fullfile(root_dir, 'data_reflector2_rx_from_initiator_measurement');
};

for k = 1:size(captures, 1)
    capture_name = captures{k, 1};
    file_path = captures{k, 2};
    x = read_complex64(file_path);

    figure('Name', capture_name);

    subplot(2, 1, 1);
    builtin('plot', abs(x));
    title([capture_name, ' - amplitude']);
    xlabel('样本点');
    ylabel('幅值');
    grid on;

    subplot(2, 1, 2);
    builtin('plot', angle(x));
    title([capture_name, ' - phase']);
    xlabel('样本点');
    ylabel('相位 (弧度)');
    grid on;
end

function x = read_complex64(file_path)
    fi = fopen(file_path, 'rb');
    if fi < 0
        error('无法打开文件: %s', file_path);
    end

    cleaner = onCleanup(@() fclose(fi));
    raw = fread(fi, 'float32');
    if mod(numel(raw), 2) ~= 0
        raw = raw(1:end-1);
    end

    x = raw(1:2:end) + 1i * raw(2:2:end);
end
