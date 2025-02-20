import {openDagsterPipes, PipesContext} from '../pipes_context';
import * as fs from 'fs';
import * as path from 'path';
import yargs from 'yargs';
import {hideBin} from 'yargs/helpers';

const TESTDATA_PATH = '../../tests/dagster-pipes-tests/src/dagster_pipes_tests/data/';

function main() {
  const args = yargs(hideBin(process.argv)).argv as any;

  using context: PipesContext = openDagsterPipes();

  switch (args.testName) {
    case 'test_message_log':
      testMessageLog(context);
      break;
    case 'test_message_report_custom_message':
      testMessageReportCustomMessage(context, args.customPayload);
      break;
    case 'test_message_report_asset_materialization':
      testMessageReportAssetMaterialization(context, args.reportAssetMaterialization);
      break;
    case 'test_message_report_asset_check':
      testMessageReportAssetCheck(context, args.reportAssetCheck);
      break;
    case 'test_error_reporting':
      testErrorReporting(context);
      break;
    default:
      break;
  }
}

function testMessageLog(context: PipesContext) {
  context.logger.debug('Debug message');
  context.logger.info('Info message');
  context.logger.warning('Warning message');
  context.logger.error('Error message');
  context.logger.critical('Critical message');
}

function testMessageReportCustomMessage(context: PipesContext, customPayloadPath: string) {
  const payload = JSON.parse(fs.readFileSync(customPayloadPath, 'utf-8')).payload;
  context.reportCustomMessage(payload);
}

function testMessageReportAssetMaterialization(
  context: PipesContext,
  assetMaterializationPath: string,
) {
  const testConfig = JSON.parse(fs.readFileSync(assetMaterializationPath, 'utf-8'));

  console.log('****' + process.cwd());

  const metadata = JSON.parse(
    fs.readFileSync(path.resolve(TESTDATA_PATH, 'static/metadata.json'), 'utf-8'),
  );

  context.reportAssetMaterialization(metadata, testConfig.dataVersion, testConfig.assetKey);
}

function testMessageReportAssetCheck(context: PipesContext, assetCheckPath: string) {
  const assetCheckConfig = JSON.parse(fs.readFileSync(assetCheckPath, 'utf-8'));

  const metadata = JSON.parse(
    fs.readFileSync(path.resolve(TESTDATA_PATH, 'static/metadata.json'), 'utf-8'),
  );

  context.reportAssetCheck(
    assetCheckConfig.checkName,
    assetCheckConfig.passed,
    assetCheckConfig.severity,
    metadata,
  );
}

function testErrorReporting(context: PipesContext) {
  context.close({
    name: 'AnError',
    message: 'Very bad error has happened!',
    stack: ['Traceback2', 'Traceback2', 'Traceback3'],
    cause: null,
    context: null,
  });

  process.exit(1);
}

main();
